# -*- coding:utf-8 -
import json
import logging
import numpy as np

from mlx_database.mongo import Mongo
from mlx_database.mysql import MySql
from mlx_message import message_manager
from mlx_server import daemon, message_server
from mlx_utility import config_manager as cm


class JDXCreditAmount(message_server.MessageServer):
    def __init__(self):
        super(JDXCreditAmount, self).__init__()
        self.queue_flow = message_manager.get_queue(cm.config['queue']['flow'])
        self.mysql_ml = MySql(**cm.config['mysql_jd_cl'])
        self.mongo_der = Mongo(**cm.config['mongo_jd_cl'])
        self.credit_config_list = self.get_credit_list()
        self.group_tag_threshold = self.get_group_tag_threshold()

    def handle_msg(self, msg_dict):
        app_id = msg_dict.get('app_id')
        category_id = msg_dict['category_id']
        model_name = msg_dict['model_name']
        model_score = msg_dict['model_score']
        group_tag = msg_dict['group_tag']
        op_score = msg_dict['op_score']

        credit_config = self.choose_one_credit(category_id, model_name)
        credit_amount, model_pass, threshold = self.get_credit_amount(credit_config, model_score)

        # 根据op_score判别控制组
        if group_tag == 'B' and model_pass == 0 and op_score >= self.group_tag_threshold:
            model_pass = 1
            credit_amount = 1000

        # 用户年龄小于22时，额度打折
        try:
            user_age = self.get_user_age(app_id)
            if int(user_age['X_IdCardAge']) < 22:
                credit_amount = round(credit_amount * 0.5)
        except:
            pass

        # 保存额度结果
        self.save_credit_results(credit_config['strategy_name'], credit_amount, app_id, category_id, model_name,
                                 credit_config['threshold_bins'], credit_config['amount_bins'])
        self.save_credit_amount_to_mongo(app_id, credit_amount)
        logging.info("app_id:{}, credit_amount:{}".format(app_id, credit_amount))

        # 更新模型結果數據
        self.update_model_result(app_id, model_name, model_pass, threshold)

        self.queue_flow.send_message({
            "app_id": app_id,
            "job_name": msg_dict['job_name'],
            "credit_amount": str(credit_amount),
            "model_pass": model_pass
        })

    def choose_one_credit(self, category_id, model_name):
        credit_list = []
        for credit_config in self.credit_config_list:
            if credit_config['category_id'] == category_id and credit_config['model_name'] == model_name:
                credit_list.append(credit_config)
        rand = np.random.random()
        credit_prob = 0
        for credit in credit_list:
            credit_prob += credit['prob']
            if rand < credit_prob:
                return credit
        return None

    def get_credit_amount(self, credit_config, score):
        amount_bins = json.loads(credit_config['amount_bins'])
        threshold_bins = json.loads(credit_config['threshold_bins'])
        if float(score) < threshold_bins[0]:
            return 0, 0, threshold_bins[0]
        for i, threshold_bin in enumerate(threshold_bins):
            if float(score) < float(threshold_bin):
                credit_amount = amount_bins[i - 1]
                break
        else:
            credit_amount = amount_bins[-1]
        return credit_amount, 1, threshold_bins[0]

    def get_credit_list(self, table='jdx_credit_configs'):
        sql = """select category_id,model_name,strategy_name,amount_bins,threshold_bins,prob from {} where is_setup=1;""".format(
            table)
        credit_list = self.mysql_ml.query(sql)
        return credit_list

    def get_group_tag_threshold(self, table='jdx_proba_tags'):
        sql = """select threshold from {} where flow_type='c' and is_setup=1;""".format(
            table)
        group_tag_threshold = self.mysql_ml.query_one(sql)
        return float(group_tag_threshold['threshold']) if group_tag_threshold else None

    def save_credit_results(self, target, credit_amount, app_id, category_id, model_name, threshold_bins, amount_bins,
                            table='jdx_credit_strategy_result'):
        sql = "insert into {} (strategy_name, credit_amount, app_id, category_id, model_name, is_judged, thresholds," \
              " segment_bins) values ('{}', {}, '{}', '{}', '{}', {},'{}', '{}')".format(
            table, target, credit_amount, app_id, category_id, model_name, 1, threshold_bins, amount_bins)
        self.mysql_ml.update(sql)

    def save_credit_amount_to_mongo(self, app_id, credit_amount, upsert=True, collection='derivables'):
        data_dict = {"X_ML_CreditAmount": credit_amount}
        self.mongo_der.get_collection(collection).update_one({"_id": app_id.upper()}, {'$set': data_dict},
                                                             upsert=upsert)

    def get_user_age(self, app_id, collection='derivables'):
        key_lists = ['X_IdCardAge']
        fields = {x: 1 for x in key_lists}
        data = self.mongo_der.get_collection(collection).find_one({'_id': app_id.upper()}, fields)
        return data

    def update_model_result(self, app_id, model_name, model_pass, threshold, table='machine_learning_jdcl_results'):
        sql = "update {} set credit_ml={}, threshold={} where app_id='{}' and model='{}';". \
            format(table, model_pass, threshold, app_id, model_name)
        return self.mysql_ml.update(sql) > 0


class JDXCreditAmountDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = JDXCreditAmount()


if __name__ == '__main__':
    JDXCreditAmountDaemon().main()
