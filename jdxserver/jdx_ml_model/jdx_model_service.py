import json
import logging
import numpy as np
import pandas as pd
import pickle
import math

from mlx_server import message_server, daemon
from mlx_utility import config_manager as cm
from mlx_database.mysql import MySql
from mlx_database.mongo import Mongo
from mlx_message import message_manager


class JDXMLCreditServer(message_server.MessageServer):
    def __init__(self):
        super(JDXMLCreditServer, self).__init__()
        self.queue_flow = message_manager.get_queue(cm.config['queue_flow']['name'])
        self.mysql_ml = MySql(**cm.config['mysql_jd_cl'])
        self.mongo_der = Mongo(**cm.config['mongo_jd_cl'])
        self.model_config_list = self.get_model_list()
        self.target = 'jd'

    def handle_msg(self, msg_dict):
        if 'app_id' not in msg_dict:
            logging.warning('No key in msg called appId!')

        app_id = str(msg_dict['app_id']).lower()
        user_id = msg_dict['user_id']
        category_id = msg_dict['category_id']

        model = self.choose_model(category_id, self.target)
        model_score = self.run_model(app_id, model)
        op_score = self.get_op_score(model_score, model['op_coef'], model['op_intercept'])

        self.save_model_result(app_id, user_id, -1, -1, {500: model_score}, '', 1,
                               -1, model['model_name'], category_id)
        self.save_op_socre_result(app_id, user_id, self.target, 'open_card', category_id, model['model_name'],
                                  model_score, op_score)

        msg = {
            "app_id": app_id,
            "job_name": msg_dict['job_name'],
            "model_name": model['model_name'],
            "op_score": op_score,
            "model_score": model_score
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
        sql = """select category_id,model_name,target,estor_path,op_coef,op_intercept,prob from {} where is_setup=1;""".format(
            table)
        model_list = self.mysql_ml.query(sql)
        return model_list

    def run_model(self, app_id, chosed_model):
        """跑模型，获得模型分数"""
        model_path = chosed_model['estor_path']
        feature_data = self.get_der_data(app_id)

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

    def save_model_result(self, app_id, user_id, credit_ml, credit_final, result, threshold, judge_by, credit_by,
                          model_name,
                          category_id, table='machine_learning_jdcl_results'):
        sql = "insert into {} " \
              "(app_id, user_id, credit_ml, credit_final, result, threshold, judge_by, credit_by, model, category_id) " \
              "values ('{}', '{}', {}, {}, '{}', '{}', {}, {}, '{}', '{}')".format(table, app_id, user_id,
                                                                                   credit_ml,
                                                                                   credit_final, json.dumps(result),
                                                                                   threshold, judge_by,
                                                                                   credit_by, model_name, category_id)
        return self.mysql_ml.update(sql) == 1

    def save_op_socre_result(self, app_id, user_id, target, server, category_id, model, model_score, op_score,
                             table='jdx_op_score_results'):
        sql = "insert into {} " \
              "(app_id, user_id, target, server, category_id, model, model_score, op_score) " \
              "values ('{}', '{}', '{}', '{}', '{}', '{}', {}, {})".format(table, app_id, user_id, target,
                                                                           server, category_id, model, model_score,
                                                                           op_score)
        return self.mysql_ml.update(sql) == 1


class JDXMLCreditServerDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = JDXMLCreditServer()


if __name__ == '__main__':
    JDXMLCreditServerDaemon().main()
