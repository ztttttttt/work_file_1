import json
import logging
import math
import numpy as np
import pandas as pd
import pickle
from mlx_server import message_server, daemon
from mlx_utility import config_manager as cm
from mlx_database.mysql import MySql
from mlx_database.mongo import Mongo
from mlx_message import message_manager


class JDXMLWithdrawModelServer(message_server.MessageServer):
    def __init__(self):
        super(JDXMLWithdrawModelServer, self).__init__()
        self.queue_flow = message_manager.get_queue(cm.config['queue_flow']['name'])
        self.mysql_ml = MySql(**cm.config['mysql_jd_cl'])
        self.mongo_der = Mongo(**cm.config['mongo_jd_cl'])
        self.model_config_list = self.get_model_list()
        self.group_tag_threshold = self.get_group_tag_threshold()

    def handle_msg(self, msg_dict):
        if 'app_id' not in msg_dict:
            logging.warning('No key in msg called appId!')

        app_id = str(msg_dict['app_id']).lower()
        user_id = msg_dict['user_id']
        category_id = msg_dict['category_id']
        group_tag = msg_dict['group_tag']

        model = self.choose_model(category_id, 'jd')
        feature_data = self.get_der_data(app_id)
        creditLine = float(feature_data.get('X_JD_CreditLine')) if feature_data.get('X_JD_CreditLine') else -1
        principal = float(feature_data.get('X_JD_Principal')) if feature_data.get('X_JD_Principal') else -1

        # 是否是预审核
        is_preAudit = 1 if feature_data.get('X_WorkFlow') == 'buyApplicationPreAudit' or feature_data.get(
            'X_isPreAudit') == 'preAudit' else 0

        model_score = self.run_model(model, feature_data)
        op_score = self.get_op_score(model_score, model['op_coef'], model['op_intercept'])
        model_pass = 0 if model_score < model['threshold'] else 1

        # 根据op_score判别控制组
        if group_tag == 'B' and model_pass == 0 and op_score >= self.group_tag_threshold:
            model_pass = 1

        self.save_model_result(app_id, user_id, category_id, creditLine, principal, model_score, model['threshold'],
                               model['model_name'], is_preAudit)
        self.save_op_socre_result(app_id, user_id, 'jd', 'withdraw', category_id, model['model_name'],
                                  model_score, op_score)

        msg = {
            "app_id": app_id,
            "job_name": msg_dict['job_name'],
            "model_pass": model_pass,
            "op_score": op_score
        }
        self.queue_flow.send_message(msg)

    def choose_model(self, category_id, target):
        models = []
        for model in self.model_config_list:
            if model['category_id'] == category_id and model['target'] == target:
                models.append(model)
        rand = np.random.random()
        model_prod = 0
        for model in models:
            model_prod += model['prob']
            if rand < model_prod:
                return model
        return None

    def get_model_list(self, table='jdx_category_model_relations'):
        sql = """select category_id,model_name,target,estor_path,threshold,op_coef,op_intercept,prob from {} where is_setup=1;""".format(
            table)
        model_list = self.mysql_ml.query(sql)
        return model_list

    def get_group_tag_threshold(self, table='jdx_proba_tags'):
        sql = """select threshold from {} where flow_type='w' and is_setup=1;""".format(
            table)
        group_tag_threshold = self.mysql_ml.query_one(sql)
        return float(group_tag_threshold['threshold']) if group_tag_threshold else None

    def run_model(self, chosed_model, feature_data):
        """跑模型，获得模型分数"""
        model_path = chosed_model['estor_path']

        with open(model_path, 'rb') as f:
            pk_obj = pickle.load(f)
            model = pk_obj['model']

        agg_attr = []
        fields = model.get_params()['enum'].clean_col_names
        for attr in fields:
            agg_attr.append(feature_data.get(attr))
        # use pandas to format data, the dimension should be 1 x len(fields)
        result_df = pd.DataFrame(agg_attr, index=fields).T
        assert result_df.ndim == 2, 'result_df should be two dimension'
        # transform data and make prediction
        pred_probas = model.predict_proba(result_df)
        model_score = pred_probas[:, 1][0]
        return float(model_score)

    def get_der_data(self, app_id, collection='derivables'):
        """获取特征数据"""
        result = {}
        try:
            result = self.mongo_der.get_collection(collection).find({'_id': app_id.upper()}).next()
            result = {key: value for key, value in result.items()}
        except Exception:
            pass
        return result

    def get_op_score(self, model_score, op_coef, op_intercept):
        if not (op_coef and op_intercept):
            return 0
        model_ln_odds = math.log(model_score / (1 - model_score)) / math.log(2)
        ln_real_odds = op_coef * model_ln_odds + op_intercept
        op_score = max(min(60 * ln_real_odds + 300, 1000), 0)
        return op_score

    def save_model_result(self, app_id, user_id, category_id, creditLine, principal, result, threshold, model_name,
                          is_preAudit, table='jdx_withdraw_model_results'):
        sql = "insert into {} " \
              "(app_id, user_id, category_id, creditLine, principal, result, threshold, model, is_preAudit) values " \
              "('{}', '{}', '{}', {}, {}, {}, {}, '{}',{})".format(table, app_id, user_id, category_id, creditLine,
                                                                   principal, result, threshold, model_name,
                                                                   is_preAudit)
        return self.mysql_ml.update(sql) == 1

    def save_op_socre_result(self, app_id, user_id, target, server, category_id, model, model_score, op_score,
                             table='jdx_op_score_results'):
        sql = "insert into {} " \
              "(app_id, user_id, target, server, category_id, model, model_score, op_score) " \
              "values ('{}', '{}', '{}', '{}', '{}', '{}', {}, {})".format(table, app_id, user_id, target,
                                                                           server, category_id, model, model_score,
                                                                           op_score)
        return self.mysql_ml.update(sql) == 1


class JDXMLWithdrawModelServerDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = JDXMLWithdrawModelServer()


if __name__ == '__main__':
    JDXMLWithdrawModelServerDaemon().main()
