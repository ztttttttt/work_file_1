import unittest
import json
import mlx_message.message_manager as mm


class QueueTest(unittest.TestCase):
    def test_send_message(self):
        mm.setup_message(
            "http://1667348596157055.mns.cn-hangzhou.aliyuncs.com",
            "LTAISSRtsh6JnpTW",
            "0AOhWeUfbkPnQf01OlGO7BnR4w47Ts")
        q = mm.get_queue("TestMachineLearningQueue")
        data = {'data': 'message from ml queue test ...'}
        result = q.send_message(json.dumps(data))
        self.assertTrue(result)

    def test_consume_message(self):
        mm.setup_message(
            "http://1667348596157055.mns.cn-hangzhou.aliyuncs.com",
            "LTAISSRtsh6JnpTW",
            "0AOhWeUfbkPnQf01OlGO7BnR4w47Ts")
        q = mm.get_queue("TestMachineLearningQueue")
        result, message = q.consume_message()
        print(message)
        self.assertTrue(result)
        self.assertIsNotNone(message)
