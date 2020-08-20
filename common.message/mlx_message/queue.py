import mns.queue as mns_queue
import mns.mns_exception as mns_exception
import logging
import json


class Queue(object):
    DEFAULT_PRIORITY = -1
    HIGH_PRIORITY = 8

    def __init__(self, queue):
        if not isinstance(queue, mns_queue.Queue):
            raise TypeError("Sorry! It has to be an instance of mns queue for the moment.")
        self.queue = queue

    def send_message(self, message_body, delay_seconds=-1, priority=DEFAULT_PRIORITY):
        if type(message_body) is dict:
            message_body = json.dumps(message_body)
        message = mns_queue.Message(message_body)
        message.set_delayseconds(delay_seconds)
        message.set_priority(priority)
        try:
            send_msg = self.queue.send_message(message)
            logging.info(
                "Send message succeed. message body: {}, message id: {}, message body Md5: {}, to queue: {}".format(
                    message_body, send_msg.message_id, send_msg.message_body_md5, self.queue.queue_name))
            return True
        except mns_exception.MNSExceptionBase:
            logging.exception("Send message fail!")
            return False

    def batch_send_message(self, messages, req_info=None):
        pass

    def peek_message(self):
        try:
            peek_message = self.queue.peek_message()
            logging.info('Peek message succeed. message body: {}'.format(peek_message.message_body))
            return True, peek_message.message_body
        except mns_exception.MNSExceptionBase:
            logging.exception("Peek message fail!")
            return False, ""

    def batch_peek_message(self, batch_size, req_info=None):
        pass

    def consume_message(self, wait_seconds=-1):
        # receive message
        receive_success, message = self.receive_message(wait_seconds)
        if not receive_success or not message:
            return False, None

        # delete message
        delete_success = self.delete_message(message.receipt_handle)
        if not delete_success:
            return False, None

        # try loading message as a python dictionary
        try:
            message_dict = json.loads(message.message_body)
            return True, message_dict
        except Exception:
            logging.exception("Load message as json failed. return raw message: {}".format(message.message_body))
            return True, message.message_body

    def batch_receive_message(self, batch_size, wait_seconds=-1, req_info=None):
        pass

    def receive_message(self, wait_seconds=-1):
        try:
            receive_msg = self.queue.receive_message(wait_seconds)
            logging.info("Receive message succeed. message body: {}, from queue: {}".format(receive_msg.message_body,
                                                                                            self.queue.queue_name))
            return True, receive_msg
        except mns_exception.MNSExceptionBase as e:
            if e.type == "MessageNotExist":
                return True, None
            logging.exception("Receive message fail!")
            return False, None
        except ConnectionResetError:
            logging.exception("Connection error!")
            return False, None

    def delete_message(self, receipt_handle):
        try:
            self.queue.delete_message(receipt_handle)
            logging.info("Delete message succeed. queue: {}".format(self.queue.queue_name))
            return True
        except mns_exception.MNSExceptionBase:
            logging.exception("Delete message fail!")
            return False

    def batch_delete_message(self, receipt_handle_list, req_info=None):
        pass

    def change_message_visibility(self, reciept_handle, visibility_timeout, req_info=None):
        pass
