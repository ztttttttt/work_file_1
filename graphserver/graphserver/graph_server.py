# -*- coding:utf-8 -
import json
import logging
from datetime import datetime

from mlx_database import mysql
from mlx_message import message_manager
from mlx_server import daemon, message_server
from mlx_utility import config_manager as cm
from ml_graph.service import graph


class MlGraphServer(message_server.MessageServer):
    def __init__(self):
        super(MlGraphServer, self).__init__()
        self.job_name = "ml_graph"
        self.stop_server = False
        self.mysql_graph = mysql.MySql(**cm.config['mysql_graph'])
        graph_min_vertices_cnt = cm.config["graph_min_vertices_cnt"]
        graph_uri = cm.config["graph_uri"]
        self.ml_graph = graph.MlGraph(self.mysql_graph, graph_min_vertices_cnt, graph_uri)
        self.ml_graph.initial_graph()
        self.last_init_time = datetime.now()

    def stop(self):
        self.stop_server = True

    def handle_msg(self, msg_dict):
        product_name = msg_dict.get('product_name')
        app_id = msg_dict.get('app_id')
        user_id = msg_dict.get('user_id')
        id_number = msg_dict.get('id_number')
        back_queue = msg_dict.get('back_queue')
        edge_types = json.loads(msg_dict.get('edge_types'))
        params = json.loads(msg_dict.get('data'))

        graph_features = self.ml_graph.cal_graph_features(app_id, product_name, user_id, id_number, edge_types, **params)
        self._send_message(product_name, app_id, graph_features, back_queue)
        # 定时重启服务,暂时计划为30天,的晚上1点开始重新初始化
        now = datetime.now()
        if (now - self.last_init_time).days >= 30 and now.hour == 1:
            self.last_init_time = datetime.now()
            self.ml_graph.initial_graph()

    def _send_message(self, product_name, app_id, graph_features, back_queue):
        queue_flow = message_manager.get_queue(back_queue)
        queue_flow.send_message({
            "app_id": app_id,
            "job_name": self.job_name,
            "product_name": product_name,
            "graph_features": graph_features
        })
        logging.info("app_id:{}, product_name:{}, graph calculate done.".format(app_id, product_name))


class MlGraphServerDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = MlGraphServer()


if __name__ == '__main__':
    MlGraphServerDaemon().main()
