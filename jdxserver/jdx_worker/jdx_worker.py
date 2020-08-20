from mlx_server import message_server, daemon
from mlx_database import mongo, mysql
from mlx_utility import config_manager as cm
from mlx_message import message_manager as mm
from worker import mx_data_worker, nineone_data_worker, inner_data_worker, mxfund_data_worker
import logging


class JDXWorker(message_server.MessageServer):
    def __init__(self):
        super().__init__()
        self.mongo_derivate = mongo.Mongo(**cm.config['mongo_derivatives'])
        self.mysql_ml = mysql.MySql(**cm.config['mysql_jd_cl'])
        self.queue_flow = mm.get_queue(cm.config['queue']['flow'])

    def handle_msg(self, msg_dict):
        app_id = msg_dict['app_id']
        derivate_data = self.get_derivate_data(app_id)
        user_id = derivate_data.get('X_UserId')
        inner_data = inner_data_worker.InnerDataWorker().run(derivate_data)
        nineone_data = nineone_data_worker.NineOneDataWorker(self.mysql_ml).run(derivate_data)
        mx_data = mx_data_worker.MxDataWorker().run(derivate_data)
        mxfund_data = mxfund_data_worker.MxFundDataWorker().run(derivate_data)
        derivatives = {}
        if inner_data:
            derivatives.update(inner_data)
        if nineone_data:
            derivatives.update(nineone_data)
        if mx_data:
            derivatives.update(mx_data)
        if mxfund_data:
            derivatives.update(mxfund_data)
        if derivatives:
            self.update_derivate_data(app_id, derivatives)

        self.queue_flow.send_message({
            'app_id': app_id,
            'user_id': user_id,
            'job_name': msg_dict['job_name']
        })

    def get_derivate_data(self, app_id, collection='derivables'):
        derivate_data = self.mongo_derivate.get_collection(collection).find_one({'_id': app_id.upper()})
        return derivate_data

    def update_derivate_data(self, app_id, data_dict, collection='derivables', upsert=True):
        self.mongo_derivate.get_collection(collection).update_one({'_id': app_id.upper()}, {'$set': data_dict},
                                                                  upsert=upsert)


class JDXWorkerDaemon(daemon.Daemon):
    def setup_server(self):
        self.server = JDXWorker()


if __name__ == '__main__':
    JDXWorkerDaemon().main()
