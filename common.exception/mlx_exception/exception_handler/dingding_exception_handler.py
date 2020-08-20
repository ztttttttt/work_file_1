from mlx_exception.exception_handler.exception_handler import ExceptionHandler
import requests
import traceback
import socket


class DingdingExceptionHandler(ExceptionHandler):
    def __init__(self, robots, env=None):
        # robots: dingding robot uri list
        self.robots = robots
        self.env = env

    def handle(self, ex=None, msg=None):
        msgs = []
        host = socket.gethostbyname(socket.gethostname())
        msgs.append('host---{}'.format(host))
        if self.env:
            msgs.append("{:-^40}".format(self.env))
        if msg:
            msgs.append(msg)
        msgs.append(traceback.format_exc())
        msg_dict = {
            "msgtype": "text",
            "text": {
                "content": '\n'.join(msgs)
            }
        }
        for rob in self.robots:
            requests.post(rob, json=msg_dict)
