import json

from mlx_server.message_server import MessageServer
from mlx_server.daemon import Daemon
from mlx_database.mysql import MySql
from mlx_message import message_manager as mm

from mlx_utility import config_manager as cm
import logging


class JDXJudger(MessageServer):
    def __init__(self):
        super().__init__()
        self.queue_flow = mm.get_queue(cm.config['queue']['flow'])
        self.mysql_ml = MySql(**cm.config['mysql_jd_cl'])

    def handle_msg(self, msg_dict):
        app_id = msg_dict['app_id']
        user_id = msg_dict['user_id']
        category_id = msg_dict['category_id']
        rule_result = msg_dict['rule_result']  #eg: 'MD1!MB9'
        model_pass = msg_dict['model_pass']
        server = msg_dict['server']

        rule_pass = 0 if rule_result else 1

        if rule_result == '' and model_pass == 1:
            final_result = 1
            result = 'Approved'
            rejectReason = ''
        elif rule_result != '' and model_pass == 1:
            final_result = 0
            result = 'Rejected'
            rejectReason = rule_result
        elif rule_result == '' and model_pass == 0:
            final_result = 0
            result = 'Rejected'
            rejectReason = 'MODL'
        else:
            final_result = 0
            result = 'Rejected'
            rejectReason = rule_result + '!MODL'

        self.save_judge_results(app_id, user_id, category_id, model_pass, rule_pass, final_result, rejectReason, server)

        msg = {
            'app_id': app_id,
            'job_name': msg_dict['job_name'],
            'final_result': final_result,
            'result': result,
            "rejectReason": rejectReason
        }
        self.queue_flow.send_message(msg)

    def save_judge_results(self, app_id, user_id, category_id, model_pass, rule_pass, final_pass, reason, server,
                           table='jdx_judge_results'):
        sql = "insert into {} (app_id, user_id, category_id, model_pass, rule_pass, final_pass, reason, server) values " \
              "('{}', '{}', '{}', {}, {}, {},'{}', '{}')".format(
            table, app_id, user_id, category_id, model_pass, rule_pass, final_pass, reason, server)
        self.mysql_ml.update(sql)


class JDXJudgerDaemon(Daemon):
    def setup_server(self):
        self.server = JDXJudger()


if __name__ == '__main__':
    JDXJudgerDaemon().main()
