
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.tree import DecisionTreeClassifier


class WoeTransform(BaseEstimator, TransformerMixin):
    def __init__(self, only_trans_str=True, max_str_values=1000, max_binning_tree_depth=3, min_bin_pct=0.02,
                 min_missing_pct_calc_woe=0.02, verbose=0, ratio_num_force_transform=0.3):
        '''
        only_trans_str - if False, all columns include numeric ones would be binned and transformed.
        max_str_values - if a columns is str type and have over max_str_values unique values, it would be discarded.
        max_bin_depth - Controls the max_depth of the decision tree used for binning.
        pct_min_bin - A bin whose percentage is under min_bin_pct should be merged.
        min_missing_pct_calc_woe - when the percentage of missing value in a columns(numeric) is less than it, the missings will not be treated as a singe bin.
                                   Instead, it will be filled with mean value.
        verbose - If positive, print logs during fitting and transformation.
        ratio_num_force_transform - if numeric ratio more than it, force transform the column to numeric
        '''
        self.only_trans_str = only_trans_str
        self.max_str_values = max_str_values
        self.max_binning_tree_depth = max_binning_tree_depth
        self.min_bin_pct = min_bin_pct
        self.min_missing_pct_calc_woe = min_missing_pct_calc_woe
        self.verbose = verbose
        self.ratio_num_force_transform = ratio_num_force_transform
        self.is_fit = False
        self.map_dict = {}
        self.nominal_col = []
        self.numeric_col = []
        self.clean_col_names = []

    def fit(self, df, y):
        df = df.copy()

        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='ignore')
            if not df[col].dtype in (int, float) and df[col].apply(
                    self.check_num).mean() >= self.ratio_num_force_transform:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                self.map_dict[col]['force_to_num'] = True
        # 1.Get the column types
        nominal_col, numeric_col = self.get_column_type(df)

        # 2.Among not numeric columns, discard columns whose value_count is over max_str_values.
        # 3.Among all columns, discard columns which null percentage is 100%.
        nominal_col_drop = []
        numeric_col_drop = []
        for col in nominal_col:
            df[col] = df[col].map(lambda x: 'NA' if pd.isnull(x) else x)
            if df[col].unique().shape[0] > self.max_str_values:
                nominal_col_drop.append(col)
        for col in numeric_col:
            if df[col].isnull().sum() == df.shape[0]:
                numeric_col_drop.append(numeric_col_drop)
        self.nominal_col = list(set(nominal_col)-set(nominal_col_drop))
        self.numeric_col = list(set(numeric_col) - set(numeric_col_drop))
        # 4.process the nominal col
        for col in nominal_col + numeric_col:
            self.map_dict[col] = {}
        df_preprocesed = self.preprocess_not_numeric_columns(df, y, nominal_col)
        clean_df = df_preprocesed[numeric_col + nominal_col]
        if self.only_trans_str:
            cols_to_bin = nominal_col
        else:
            cols_to_bin = numeric_col + nominal_col
        for col in cols_to_bin:
            woe_mapper, threshold, mean_fillna = self.auto_binning(clean_df, y, col, max_binning_tree_depth=self.max_binning_tree_depth)
            self.map_dict[col]['woe_mapper'] = woe_mapper
            self.map_dict[col]['threshold'] = threshold
            self.map_dict[col]['mean_fillna'] = mean_fillna
        self.clean_col_names = self.nominal_col + self.numeric_col
        self.is_fit = True
        return self

    def transform(self, df):
        if not self.is_fit:
            raise Exception('Please fit before transform!')
        transform_df = df.copy()
        for col in self.clean_col_names:
            if col not in transform_df.columns:
                transform_df[col] = np.nan
        for col in transform_df.columns:
            transform_df[col] = pd.to_numeric(transform_df[col], errors='ignore')
            if self.map_dict.get('force_to_num'):
                transform_df[col] = pd.to_numeric(transform_df[col], errors='coerce')
        if self.only_trans_str:
            cols_to_bin = self.nominal_col
        else:
            cols_to_bin = self.numeric_col + self.nominal_col

        for col in cols_to_bin:
            if col in transform_df.columns:
                fillna_mean = self.map_dict.get(col).get('mean_fillna')
                if fillna_mean:
                    transform_df[col] = transform_df[col].fillna(fillna_mean)
                nominal_mapper = self.map_dict.get(col).get('nominal_mapper')
                if nominal_mapper:
                    transform_df[col] = transform_df[col].map(lambda x: 'NA' if pd.isnull(x) else x)
                    replace_dict = self.map_dict.get(col).get('replace_dict')
                    if replace_dict:
                        transform_df[col] = transform_df[col].map(lambda x: replace_dict.get(str(x), x))
                    transform_df[col] = transform_df[col].map(lambda x: nominal_mapper.get(str(x), x))
                    # if find a new element, transform it to null number
                    transform_df[col] = transform_df[col].map(lambda x: nominal_mapper.get('NA')
                    if isinstance(x, str) else x)
                threshold = self.map_dict.get(col).get('threshold')
                transform_df[col] = pd.cut(transform_df[col], threshold, right=False)
                if transform_df[col].isnull().sum() > 0:
                    transform_df[col] = transform_df[col].cat.add_categories(['NA'])
                    transform_df[col] = transform_df[col].fillna('NA')
                woe_mapper = self.map_dict.get(col).get('woe_mapper')
                transform_df[col].replace(woe_mapper, inplace=True)
                transform_df[col] = pd.to_numeric(transform_df[col], errors='coerce')

        return transform_df[self.clean_col_names]

    def get_column_type(self, df):
        nominal_col = []
        numeric_col = []
        for key, value in df.dtypes.to_dict().items():
            if value != 'object':
                numeric_col.append(key)
            else:
                nominal_col.append(key)
        return nominal_col, numeric_col

    def preprocess_not_numeric_columns(self, df, y, nominal_col):
        for col in nominal_col:
            # 1.Bins whose percentage under min_bin_pct should be merged as one bin.
            sr_col_pct = df[col].value_counts() / df.shape[0]
            sr_col_pct_min_bin_pct = sr_col_pct[sr_col_pct < self.min_bin_pct]
            if not sr_col_pct_min_bin_pct.empty:
                replace_dict = dict(
                    zip(sr_col_pct_min_bin_pct.index, ['BIN_min_bin_pct', ] * len(sr_col_pct_min_bin_pct)))
                df[col].replace(replace_dict, inplace=True)
                self.map_dict[col]['replace_dict'] = replace_dict
            # 2.Map the bin to the bad rate = 1-y.
            bad_rate_dict = dict((1 - y).groupby(df[col]).mean())
            df[col].replace(bad_rate_dict, inplace=True)
            self.map_dict[col]['nominal_mapper'] = bad_rate_dict
        return df

    def auto_binning(self, df, y, col, max_binning_tree_depth=4):
        # Columns in ls_cols_to_bin should all be numeric, including those transformed from not numeric ones.

        df_col_y = pd.DataFrame({col: df[col], 'y': y})
        df_col_y_notnull = df_col_y[df_col_y[col].map(lambda x: not pd.isnull(x))].copy()
        tree_clf = DecisionTreeClassifier(criterion='entropy', max_depth=max_binning_tree_depth)
        tree_clf.fit(df_col_y_notnull[col].values.reshape(-1, 1), df_col_y_notnull['y'].values.reshape(-1, 1))
        bin_threshold = [-np.inf, ] + sorted(
            list(set(tree_clf.tree_.threshold[tree_clf.tree_.threshold != -2]))) + [np.inf, ]
        # if the null bin percentage is less then 2%, fill then with mean value
        mean_fillna = None
        if 0 < df_col_y[col].isnull().sum() / df_col_y.shape[0] < self.min_missing_pct_calc_woe:
            mean_fillna = df_col_y[col].mean()
            df_col_y[col] = df_col_y[col].fillna(mean_fillna)
            df_col_y['bin'] = pd.cut(df_col_y[col], bin_threshold, right=False)
        elif df_col_y[col].isnull().sum() == 0:
            df_col_y['bin'] = pd.cut(df_col_y[col], bin_threshold, right=False)
        else:
            df_col_y['bin'] = pd.cut(df_col_y[col], bin_threshold, right=False)
            df_col_y['bin'] = df_col_y['bin'].cat.add_categories(['NA'])
            df_col_y['bin'] = df_col_y['bin'].fillna('NA')
        sr_woe_mapper = self.calc_woe(df_col_y['bin'], df_col_y['y'])
        dct_bin_woe_mapper = dict(zip(sr_woe_mapper.index, sr_woe_mapper))
        return dct_bin_woe_mapper, bin_threshold, mean_fillna

    def calc_woe(self, x_bin, y):
        df_xy = pd.DataFrame({'x': x_bin, 'y': y})
        bad_num = sum(y)
        good_num = len(y) - bad_num
        bad_dist = df_xy.groupby('x')['y'].apply(sum) / bad_num + 0.01
        good_dist = (df_xy.groupby('x')['y'].apply(len) - df_xy.groupby('x')['y'].apply(sum)) / good_num + 0.01
        sr_woe = (bad_dist / good_dist).apply(np.log)
        # iv = round(sum((bad_dist - good_dist)*sr_woe),3)
        return sr_woe

    def check_num(self, s):
        s = str(s)
        if s.isdecimal():
            return True
        elif s.count('.') == 1:
            left, right = s.split('.')
            if left.isdecimal() and right.isdecimal():
                return True
        else:
            return False
