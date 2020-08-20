# -*- coding:utf-8 -
import json
import logging
import traceback

import numpy as np

from interval import Interval
from mlx_database.mongo import Mongo
from mlx_database.mysql import MySql
from mlx_message import message_manager
from mlx_server import daemon, message_server
from mlx_utility import config_manager as cm


class JDXModifyQuota(message_server.MessageServer):
    def __init__(self):
        super(JDXModifyQuota, self).__init__()
        self.queue_flow = message_manager.get_queue(cm.config['queue_flow']['name'])
        self.mysql_ml = MySql(**cm.config['mysql_jd_cl'])
        self.mongo_derivable = Mongo(**cm.config['mongo_jd_cl'])
        self.quota_modify_strategy_list = self.get_quota_modify_strategy_list()  # 获取全部额度变更策略配置

    def handle_msg(self, msg_dict):
        app_id = msg_dict['app_id']
        category_id = msg_dict['category_id']
        model_score = msg_dict['model_score']  # 预测的模型分
        model_name = msg_dict['model_name']  # 预测的模型名
        data = self.get_user_data_by_app_id(app_id)
        user_id = data.get('X_UserId')
        try:
            original_quota = int(float(data.get('X_JD_CreditLine')))  # 用户当前额度
            original_principal = int(float(data.get('X_pre_app_principal')))  # 用户最后一次的提现金额
            quota_modify_count = int(data.get('X_credit_adjustment_times'))  # 提额次数
            # 根据模型分和当前额度选择一个额度变更策略
            quota_modify_strategy = self.get_quota_modify_strategy(category_id, model_score, original_quota)
            # 根据额度变更策略确定额度的变化标签
            modify_tag = self.get_modify_tag(quota_modify_strategy)

            # 最终额度
            if modify_tag >= 0:
                final_quota = round(original_quota + original_principal * modify_tag)
            else:
                final_quota = round(original_quota * (1 + modify_tag))

            # 用户年龄小于22时，额度打折
            try:
                if int(data['X_IdCardAge']) < 22:
                    final_quota = round(final_quota * 0.5)
            except:
                pass

            # 额度变化值
            quota_variance = final_quota - original_quota

            logging.info("app_id:{}, final_credit:{}".format(app_id, final_quota))
            self.queue_flow.send_message({
                "app_id": app_id,
                "job_name": msg_dict['job_name'],
                "final_quota": str(final_quota)
            })
            # 保存额度变更结果
            self.save_quota_modify_results(app_id, user_id, category_id, quota_modify_strategy['modify_name'],
                                           model_score,
                                           model_name, original_quota, original_principal,
                                           quota_modify_strategy['score_segment'],
                                           quota_modify_strategy['quota_segment'], quota_modify_strategy['level'], quota_variance, final_quota,
                                           quota_modify_count)
        except:
            logging.warning(traceback.format_exc())
            final_quota = int(float(data.get('X_JD_CreditLine')))
            quota_variance = 0
            logging.info("app_id:{}, final_credit:{}".format(app_id, final_quota))
            self.queue_flow.send_message({
                "app_id": app_id,
                "job_name": msg_dict['job_name'],
                "final_quota": str(final_quota)
            })
            # 保存额度变更结果
            self.save_quota_modify_results(app_id, user_id, category_id, '', model_score, model_name,
                                           data.get('X_JD_CreditLine'), -1, '', '', 0, quota_variance, final_quota,
                                           data.get('X_credit_adjustment_times'))

    def get_quota_modify_strategy(self, category_id, model_score, original_quota):
        for quota_modify_strategy in self.quota_modify_strategy_list:
            score_segment = json.loads(quota_modify_strategy['score_segment'])
            quota_segment = json.loads(quota_modify_strategy['quota_segment'])
            score_in = model_score in Interval(score_segment[0], score_segment[1], upper_closed=False)
            quota_in = original_quota in Interval(quota_segment[0], quota_segment[1], upper_closed=False)
            if quota_modify_strategy['category_id'] == category_id and score_in and quota_in:
                return quota_modify_strategy

    def get_modify_tag(self, quota_modify_strategy):
        action_space = json.loads(quota_modify_strategy['action_space'])
        action_prob = json.loads(quota_modify_strategy['action_prob'])
        rand = np.random.random()
        modify_prob = 0
        for i, prob in enumerate(action_prob):
            modify_prob += prob
            if rand < modify_prob:
                modify_tag = action_space[i]
                return modify_tag

    def save_quota_modify_results(self, app_id, user_id, category_id, modify_name, model_score, model_name,
                                  original_quota, original_principal, score_segment,
                                  quota_segment, level, quota_variance, final_quota, quota_modify_count,
                                  table='jdx_quota_modify_results'):
        sql = "insert into {} (app_id,user_id,category_id,modify_name,model_score,model_name,original_quota," \
              "original_principal,score_segment,quota_segment,level,quota_variance,final_quota,quota_modify_count) " \
              "values ('{}', '{}', '{}', '{}', {}, '{}',{},{}, '{}','{}',{},{},{},{})". \
            format(table, app_id, user_id, category_id, modify_name, model_score, model_name, original_quota,
                   original_principal,score_segment, quota_segment,level,quota_variance, final_quota, quota_modify_count)
        self.mysql_ml.update(sql)

    def get_quota_modify_strategy_list(self, table='jdx_quota_modify_configs'):
        sql = "select modify_name,category_id,score_segment,quota_segment,level,action_space,action_prob " \
              "from {} where is_setup=1;" \
            .format(table)
        quota_modify_strategy_list = self.mysql_ml.query(sql)
        return quota_modify_strategy_list

    def get_user_data_by_app_id(self, app_id, collection='derivables'):
        key_lists = ['X_UserId', 'X_JD_CreditLine', 'X_pre_app_principal', 'X_credit_adjustment_times',
                     'X_IdCardAge']
        fields = {x: 1 for x in key_lists}
        data = self.mongo_derivable.get_collection(collection).find_one({'_id': app_id.upper()}, fields)
        return data


class JDXModifyQuotaDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = JDXModifyQuota()


if __name__ == '__main__':
    JDXModifyQuotaDaemon().main()
