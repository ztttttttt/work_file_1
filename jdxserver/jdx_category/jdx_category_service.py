import logging
import json
from mlx_server import message_server, daemon
from mlx_message import message_manager as mm
from mlx_utility import config_manager as cm
from mlx_database.mysql import MySql
from mlx_database.mongo import Mongo
from asteval import Interpreter


class JdxCategoryService(message_server.MessageServer):
    def __init__(self):
        super().__init__()
        self.queue_flow = mm.get_queue(cm.config['queue_flow']['name'])
        self.mysql_ml = MySql(**cm.config['mysql_jd_cl'])
        self.mongo_derivate = Mongo(**cm.config['mongo_jd_cl'])
        self.category_list = self.get_category_configs()

    def handle_msg(self, msg_dict):
        app_id = msg_dict['app_id']
        user_id = msg_dict['user_id']
        derivate_data = self.get_derivate_data(app_id)
        channel = derivate_data.get('X_Origin_source')
        workflow, flow_type = self.get_workflow(derivate_data)
        category_id = self.get_category_id(derivate_data, workflow)
        msg_out = {
            "app_id": app_id,
            "job_name": msg_dict['job_name'],
            "category_id": category_id,
        }
        self.queue_flow.send_message(msg_out)

        self.save_category_results(app_id, user_id, category_id, channel, flow_type)

    def get_category_configs(self, table='jdx_category_relations'):
        sql = """SELECT category_id,relation from {} WHERE is_setup=1;""".format(table)
        category_list = self.mysql_ml.query(sql)
        return category_list

    def get_derivate_data(self, app_id, collection='derivables'):
        der_dict = self.mongo_derivate.get_collection(collection).find_one({'_id': app_id.upper()})
        return der_dict

    def save_category_results(self, app_id, user_id, category_id, channel, flow_type, table='jdx_category_results'):
        sql = """INSERT INTO {} (app_id,user_id,category_id,channel,flow_type) VALUES('{}','{}','{}','{}','{}');""".format(table,
                                                                                                            app_id,
                                                                                                            user_id,
                                                                                                            category_id,
                                                                                                            channel,
                                                                                                            flow_type)
        self.mysql_ml.update(sql)

    def get_category_id(self, derivate_data, workflow):
        aeval = Interpreter()
        category_id = None
        default_category_id = None
        for category in self.category_list:
            if category['relation'] == workflow + '_default':
                default_category_id = category['category_id']
            try:
                relation_dict = json.loads(category['relation'])
                flag = []
                for k, v in relation_dict.items():
                    aeval.symtable['VALUE'] = derivate_data.get(k)
                    flag.append(aeval(v))
                if all(flag):
                    category_id = category['category_id']
                    break
            except:
                    pass
        if not category_id:
            category_id = default_category_id
        return category_id

    def get_workflow(self, derivate_data):
        x_workflow = derivate_data['X_WorkFlow_flag']
        mx_data = derivate_data.get('X_MX_RawReport')
        if x_workflow == 'card':
            workflow = 'opencard'
            flow_type = 'c'
        elif x_workflow == 'reloan' and mx_data:
            workflow = 'reloanwithdraw_mx'
            flow_type = 'w'
        elif x_workflow == 'reloan' and not mx_data:
            workflow = 'reloanwithdraw'
            flow_type = 'w'
        elif x_workflow == 'quota':
            workflow = 'quota'
            flow_type = 'q'
        elif x_workflow == 'firstloan':
            workflow = 'firstwithdraw'
            flow_type = 'f'
        return workflow, flow_type


class JdxCategoryServiceDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = JdxCategoryService()


if __name__ == '__main__':
    JdxCategoryServiceDaemon().main()
