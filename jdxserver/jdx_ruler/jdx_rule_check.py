from mlx_server.message_server import MessageServer
from mlx_server.daemon import Daemon
from mlx_database.mysql import MySql
from mlx_database.mongo import Mongo
from mlx_utility import config_manager as cm
from mlx_message import message_manager as mm
import json
import numpy as np


class JdxRuler(MessageServer):
    def __init__(self):
        super().__init__()
        self.queue_flow = mm.get_queue(cm.config['queue_flow']['name'])
        self.mysql_ml = MySql(**cm.config['mysql_jd_cl'])
        self.mongo_derivate = Mongo(**cm.config['mongo_jd_cl'])
        self.rule_config_list = self.get_rule_list()
        self.group_tag_config_list = self.get_group_tag_list()

    def handle_msg(self, msg_dict):
        app_id = str(msg_dict['app_id'])
        user_id = msg_dict.get('user_id', '')
        category_id = msg_dict.get('category_id', '')
        der_data = self.get_der_data(app_id)
        group_tag, flow_type = self.get_group_tag_and_flow_type(der_data)  # 控制组标签
        rule_check_result = self.get_rule_check_result(category_id, flow_type, group_tag, der_data)
        hit_rule_str = "!".join(rule_check_result['hit_rule_code'])
        msg = {
            "app_id": app_id,
            "job_name": msg_dict['job_name'],
            "group_tag": group_tag,
            "rule_pass": rule_check_result['result'],
            "hit_rule_str": hit_rule_str
        }
        self.queue_flow.send_message(msg)
        saved_data = {
            'app_id': app_id,
            'user_id': user_id,
            'category_id': category_id,
            'hit_rule_code': rule_check_result['hit_rule_code'],
            'actual_hit_rule_code': rule_check_result['actual_hit_rule_code'],
            'result': rule_check_result['result'],
            'actual_result': rule_check_result['actual_result'],
            'group_tag': group_tag,
            'flow_type': flow_type
        }
        self.save_rule_result(saved_data)

    def get_rule_list(self, table='jd_ml_rules'):
        """获取所有规则配置信息"""
        sql = """select category_id,rule_code,pass_prob,rule_params,flow_type,group_tag from {} where is_setup=1;""".format(
            table)
        rule_list = self.mysql_ml.query(sql)
        return rule_list

    def get_der_data(self, app_id, collection='derivables'):
        """获取特征数据"""
        result = {}
        try:
            result = self.mongo_derivate.get_collection(collection).find({'_id': app_id.upper()}).next()
            result = {key: value for key, value in result.items()}
        except Exception:
            pass
        return result

    def save_rule_result(self, saved_data, table='jd_ml_rule_check_result'):
        """保存规则检查结果"""
        app_id = saved_data.get('app_id')
        user_id = saved_data.get('user_id', '')
        category_id = saved_data.get('category_id')
        hit_rule_code = json.dumps(saved_data.get('hit_rule_code'))
        actual_hit_rule_code = json.dumps(saved_data.get('actual_hit_rule_code'))
        result = saved_data.get('result')
        actual_result = saved_data.get('actual_result')
        group_tag = saved_data.get('group_tag')
        flow_type = saved_data.get('flow_type')

        sql = "insert into {} (app_id,user_id,category_id,hit_rule_code,actual_hit_rule_code,result,actual_result," \
              "group_tag,rule_type) values ('{}','{}','{}','{}','{}',{},{},'{}','{}')".format(table, app_id, user_id,
                                                                                              category_id,
                                                                                              hit_rule_code,
                                                                                              actual_hit_rule_code,
                                                                                              result, actual_result,
                                                                                              group_tag, flow_type)
        self.mysql_ml.update(sql)

    def get_rule_check_result(self, category_id, flow_type, group_tag, der_data):
        """得到规则检测结果"""
        if category_id not in [rule['category_id'] for rule in self.rule_config_list]:
            if flow_type == 'c':
                category_id = 'default_opencard'
            elif flow_type == 'f':
                category_id = 'default_firstloan'
            elif flow_type == 'w':
                category_id = 'default_reloan'

        rule_list = []
        for rule_config in self.rule_config_list:
            if rule_config['category_id'] == category_id and rule_config['flow_type'] == flow_type and rule_config[
                'group_tag'] == group_tag:
                rule_list.append(rule_config)

        hit_rule_code = []
        actual_hit_rule_code = []
        for rule in rule_list:
            rule_code = rule['rule_code']
            pass_prob = int(rule['pass_prob'])
            rule_params = rule['rule_params']
            rule_params = json.loads(rule_params)

            check_list = []
            for rule_dict in rule_params['con']:
                try:
                    f_value = der_data.get(rule_dict['field'])
                    s_value = rule_dict.get('val')
                    op = rule_dict.get('op')
                    flag = self.__condition_exec(f_value, s_value, op)
                except:
                    flag = False
                check_list.append(flag)

            check_result = False
            if rule_params['log_op'] == 'and':
                check_result = all(check_list)
            if rule_params['log_op'] == 'or':
                check_result = any(check_list)

            if check_result:
                hit_rule_code.append(rule_code)
                actual_hit_rule_code.append(rule_code)

                prob = np.random.random()
                if prob < pass_prob:
                    hit_rule_code.remove(rule_code)
        result = 0 if hit_rule_code else 1
        actual_result = 0 if actual_hit_rule_code else 1
        rule_check_result = {
            'hit_rule_code': hit_rule_code,
            'actual_hit_rule_code': actual_hit_rule_code,
            'result': result,
            'actual_result': actual_result,
        }
        return rule_check_result

    def get_group_tag_list(self, table='jdx_proba_tags'):
        """获取控制组"""
        sql = """select tag,proba,flow_type from {} where is_setup=1;""".format(table)
        group_tag_list = self.mysql_ml.query(sql)
        return group_tag_list

    def get_group_tag_and_flow_type(self,der_data):
        X_WorkFlow_flag = der_data.get('X_WorkFlow_flag')
        if X_WorkFlow_flag == 'card':
            flow_type = 'c'
        elif X_WorkFlow_flag == 'firstloan':
            flow_type = 'f'
        elif X_WorkFlow_flag == 'reloan':
            flow_type = 'w'

        for tag_dict in self.group_tag_config_list:
            if tag_dict['flow_type'] == flow_type:
                tag_config = tag_dict
        prob = np.random.random()
        # B表示为控制组
        if prob < float(tag_config['proba']):
            group_tag = 'B'
        else:
            group_tag = 'A'
        return group_tag, flow_type

    def __condition_exec(self, f_value, s_value, op):
        result = False
        if op == '==':
            result = f_value == s_value
        elif op == '>':
            result = float(f_value) > float(s_value)
        elif op == '>=':
            result = float(f_value) >= float(s_value)
        elif op == '<':
            result = float(f_value) < float(s_value)
        elif op == '<=':
            result = float(f_value) <= float(s_value)
        elif op == '!=':
            result = f_value != s_value
        elif op == 'in':
            result = f_value in s_value
        elif op == 'nin':
            result = f_value not in s_value
        return result


class JdxRulerDaemon(Daemon):
    def setup_server(self):
        self.server = JdxRuler()


if __name__ == '__main__':
    JdxRulerDaemon().main()
