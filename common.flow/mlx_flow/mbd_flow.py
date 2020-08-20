import logging
from mlx_message import message_manager
import copy
import time


class MBDFlowManager:
    def __init__(self, mongo_client, mongo_collection, default_flow=None):
        self.mongo_client = mongo_client
        self.mongo_collection = mongo_collection
        self.default_flow = default_flow
        self.mbd_flow_map = {}

    def register_flow(self, mbd_flow):
        # flow: instance of MBDFlow
        self.mbd_flow_map[mbd_flow['flow_name']] = MBDFlow(mbd_flow)

    def receive_message(self, app_id, msg_dict):
        logging.info("MBDFlowManager rece msg. app_id="+app_id)
        flow_record = self.__get_flow_record(app_id)
        if not flow_record:
            flow_name = self.__extract_flow_name_from_msg(msg_dict)
            flow_record = self.__create_flow_record(app_id, flow_name)
        if not flow_record.get('flow_name'):
            logging.error("flow record has no flow name!")
            return
        flow_record = self.mbd_flow_map[flow_record['flow_name']].receive_message(flow_record, msg_dict)
        self.__update_flow_record(flow_record)

    def __get_flow_record(self, app_id):
        query = {'app_id': str(app_id).lower()}
        return self.mongo_client.get_collection(self.mongo_collection).find_one(query)

    def __create_flow_record(self, app_id, flow_name):
        flow_record = {
            'app_id': str(app_id).lower(),
            'flow_name': flow_name,
            'create_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            'finished_flows': [],
            'received_messages': [],
            'sent_messages': [],
            'variables': {}  # variables are recorded by flow and can be used in the following flows
        }
        self.mongo_client.get_collection(self.mongo_collection).insert_one(flow_record)
        return flow_record

    def __update_flow_record(self, flow_record, retry=2):
        app_id = str(flow_record['app_id']).lower()
        result = self.mongo_client.get_collection(self.mongo_collection).replace_one(
            {'app_id': app_id}, flow_record, upsert=True)
        logging.info('update flow record result--- app_id: {}, result: {}'.format(app_id, result.modified_count))
        if result.modified_count == 0:
            logging.error("update flow record failed. app_id: {}, retry: {}".format(app_id, retry))
            retry -= 1
            if retry > 0:
                self.__update_flow_record(flow_record, retry)

    def __extract_flow_name_from_msg(self, msg_dict):
        if msg_dict.get('data') and msg_dict['data'].get('status'):
            return msg_dict['data']['status']
        return self.default_flow


# message based distributed flow
class MBDFlow:
    def __init__(self, flow_data):
        self.flows = flow_data['flows']
        self.receive_message_flows = [f for f in self.flows if f['type'] == MBDFlowType.RECEIVE_MESSAGE]
        self.send_message_flows = [f for f in self.flows if f['type'] == MBDFlowType.SEND_MESSAGE]
        self.dependencies = flow_data['dependencies']

    def receive_message(self, flow_record, msg_dict):
        logging.info("mbd rece msg="+str(msg_dict))
        flow = self.__get_matched_flow_by_message(msg_dict)
        if not flow:
            logging.warning("received message not match any flow. message: %s", msg_dict)
            return

        if 'vars' in flow:
            for var in flow['vars']:
                flow_record['variables'][var['var_key']] = \
                    self.__get_value_from_dict_by_dot_separated_keys(var['msg_key'], msg_dict)
        flow_record['received_messages'].append(msg_dict)
        flow_record['finished_flows'].append(flow['name'])

        logging.info("mbd rece start to send msg")
        # send messages
        for sm_flow in self.send_message_flows:
            smfn = sm_flow['name']
            if smfn in flow_record['finished_flows']:
                continue
            for dep in self.dependencies:
                if smfn in dep['downstream'] and set(flow_record['finished_flows']).issuperset(set(dep['upstream'])):
                    sm_flow_copy = copy.deepcopy(sm_flow)
                    self.__send_message(sm_flow_copy, flow_record)
        return flow_record

    def __send_message(self, flow, flow_record):
        queue = message_manager.get_queue(flow['queue'], with_new_client=True)
        message_tpl = flow['message'].copy()
        message_to_send = self.__build_message(message_tpl, flow_record['variables'])
        queue.send_message(message_to_send, delay_seconds=flow.get('delayed_seconds', -1))
        flow_record['sent_messages'].append(message_to_send)
        flow_record['finished_flows'].append(flow['name'])

    def __build_message(self, message, variables):
        for key, value in message.items():
            if isinstance(value, tuple):
                msg_type = value[0]
                msg_value = value[1]
                if msg_type == "val":
                    message[key] = msg_value
                elif msg_type == "var":
                    if msg_value == MBDFlowBuildInVariable.NOW_IN_MILLISECOND:
                        message[key] = int(time.time() * 1000)
                    else:
                        message[key] = variables.get(msg_value, None)
            elif isinstance(value, list):
                for v in value:
                    self.__build_message(v, variables)
            else:
                self.__build_message(message[key], variables)
        return message

    def __get_matched_flow_by_message(self, msg_dict):
        for flow in self.receive_message_flows:
            if (flow['match'].items() <= msg_dict.items()) or \
                    ('data' in flow['match'] and 'data' in msg_dict and flow['match']['data'].items() <= msg_dict['data'].items()):
                return flow
        return None

    @classmethod
    def __get_value_from_dict_by_dot_separated_keys(cls, dot_key, dict_data):
        # dot_key example: a.b.c
        # dict_data example: {'a': {'b': {'c': 1}}}
        keys = dot_key.split('.')
        value = dict_data.copy()
        for k in keys:
            value = value[k]
        return value


class MBDFlowType:
    RECEIVE_MESSAGE = 0  # receive message in, thus "match" key is required
    SEND_MESSAGE = 1  # send message out, thus "queue" & "message" keys are required


class MBDFlowBuildInVariable:
    NOW_IN_MILLISECOND = "MBD_NOW_IN_MILLISECOND"
