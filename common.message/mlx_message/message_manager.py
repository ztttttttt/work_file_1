from mns import mns_client, queue as mns_queue, topic as mns_topic
from mlx_message import queue, topic

__mns_client = None
__host, __access_id, __access_key = None, None, None
__logger = None


def setup_message(host, access_id, access_key, security_token="", logger=None):
    global __mns_client, __host, __access_id, __access_key, __logger
    __host, __access_id, __access_key = host, access_id, access_key
    __logger = logger
    __mns_client = mns_client.MNSClient(
        host, access_id, access_key, security_token=security_token, logger=logger)


def get_queue(queue_name, with_new_client=False):
    client = __mns_client
    if with_new_client:
        client = mns_client.MNSClient(__host, __access_id, __access_key, logger=__logger)
        client.set_keep_alive(False)
    mns_q = mns_queue.Queue(queue_name, client)
    return queue.Queue(mns_q)


def get_topic(topic_name, with_new_client=False):
    client = __mns_client
    if with_new_client:
        client = mns_client.MNSClient(__host, __access_id, __access_key, logger=__logger)
        client.set_keep_alive(False)
    mns_t = mns_topic.Topic(topic_name, client)
    return topic.Topic(mns_t)
