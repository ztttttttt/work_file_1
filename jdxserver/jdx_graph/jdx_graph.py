# -*- coding:utf-8 -
import json
import logging
from mlx_database import mongo
from mlx_message import message_manager
from mlx_server import daemon, message_server
from mlx_utility import config_manager as cm
from mlx_jdx.dal import jdx_data


class JDXGraphFeatures(message_server.MessageServer):
    def __init__(self):
        super(JDXGraphFeatures, self).__init__()
        self.job_name = "jdx_graph_features"
        self.queue_flow = message_manager.get_queue(cm.config['queue']['flow'])
        self.queue_name = cm.config['queue_server']['name']
        self.queue_ml_graph = message_manager.get_queue(cm.config['queue']['ml_graph'])
        self.stop_server = False
        self.mongo_client = mongo.Mongo(**cm.config['mongo_jd_cl'])
        self.jdx_data = jdx_data.JDXData(self.mongo_client)

    def stop(self):
        self.stop_server = True

    def handle_msg(self, msg_dict):
        job_name = msg_dict.get('job_name')
        if job_name == self.job_name:
            app_id = msg_dict.get('app_id')
            product_name = msg_dict.get('product_name')
            # edge_types = ["contactbook", "gps"]
            edge_types = ["contactbook", "gps", "device_id", "base_station", "bssid"]
            params = ['X_UserId', 'X_JD_Latitude', 'X_JD_Longtitude', 'X_User_addressBook', 'X_MobileService_ContactList',
                      'X_IdCardNum', 'X_Mobile', 'create_time', 'X_JD_Amount', 'X_JD_TraceIp', 'X_JD_OS', 'X_JD_DeviceExtend']
            der_data = self.jdx_data.get({'_id': app_id.upper()}, params)

            contactbook = self._merge_contactlist(der_data.get('X_User_addressBook'), der_data.get('X_MobileService_ContactList'))
            longitude = float(der_data.get('X_JD_Longtitude')) if der_data.get('X_JD_Longtitude') else None
            latitude = float(der_data.get('X_JD_Latitude')) if der_data.get('X_JD_Latitude') else None
            user_id = der_data.get('X_UserId')
            id_number = der_data.get('X_IdCardNum')
            mobile = der_data.get('X_Mobile')
            mobile_os = der_data.get('X_JD_OS')
            bssid = der_data.get('X_JD_TraceIp')
            extend = der_data.get('X_JD_DeviceExtend')
            device_id = None
            base_station = None
            if extend:
                device_id = extend.get('deviceId')
                base_station = extend.get('cid')
            application_time = der_data.get('create_time')
            # deviceid 需要采集
            # cann't get principal
            principal = der_data.get('X_JD_Amount', 0)  # if is activate card then without any principal
            params = {
                'mobile': mobile,
                'mobile_os': mobile_os,
                'contactbook': contactbook,
                'longitude': longitude,
                'latitude': latitude,
                'bssid': bssid,
                'device_id': device_id,
                'base_station': base_station,
                'principal': 0,
                'application_time': str(application_time)
            }
            self._send_to_ml_graph(app_id, user_id, id_number, product_name, edge_types, self.queue_name, **params)
        elif job_name == 'ml_graph':
            graph_features = msg_dict.get('graph_features')
            app_id = msg_dict.get('app_id')
            self._save_and_send_message(app_id, graph_features)
        else:
            logging.error('Wrong job_name to jdx_graph. job_name:{}'.format(job_name))

    def _merge_contactlist(self, addressbook, mx_contractlist):
        contactbook = []
        if addressbook:
            addressbook = json.loads(addressbook)
            if addressbook:
                if type(addressbook) is list:
                    contactbook = addressbook[0].get('contents')
                else:
                    contactbook = addressbook.get('contents')
        mobiles = [r['mobile'] for r in contactbook]
        if mx_contractlist:
            mx_contactbook = [{'mobile': r['phone_num'], 'name': 'Unknow'} for r in mx_contractlist if r['phone_num'] not in mobiles]
            contactbook.extend(mx_contactbook)
        contactbook = [r['mobile'] for r in contactbook]
        return contactbook

    def _send_to_ml_graph(self, app_id, user_id, id_number, product_name, edge_types, back_queue, **params):
        self.queue_ml_graph.send_message({
            'app_id': app_id,
            'user_id': user_id,
            'id_number': id_number,
            'product_name': product_name,
            'back_queue': back_queue,
            'edge_types': json.dumps(edge_types),
            'data': json.dumps(params)
        })
        logging.info('send to ml graph: {}'.format(app_id))

    def _save_and_send_message(self, app_id, graph_features):
        self.jdx_data.update_one({'_id': app_id.upper()}, graph_features)
        self.queue_flow.send_message({
                "app_id": app_id,
                "job_name": "jdx_graph_features"
            }
        )
        logging.info('Save graph features to mongo done.')


class JDXGraphFeaturesDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = JDXGraphFeatures()


if __name__ == '__main__':
    JDXGraphFeaturesDaemon().main()
