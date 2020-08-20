import unittest
import mlx_message.message_manager as mm


class TopicTest(unittest.TestCase):
    def test_publish_message(self):
        mm.setup_message(
            "http://1667348596157055.mns.cn-hangzhou.aliyuncs.com",
            "sulgyCs1lNU420ra",
            "QbXX86FSRUNHEMyEGXfHznJ00xeeop")
        tp = mm.get_topic("TestTopic")
        result = tp.publish_message("test message from ml ...")
        self.assertTrue(result)

    def test_subscribe_queue_end_point(self):
        mm.setup_message(
            "http://1667348596157055.mns.cn-hangzhou.aliyuncs.com",
            "sulgyCs1lNU420ra",
            "QbXX86FSRUNHEMyEGXfHznJ00xeeop")
        tp = mm.get_topic("TestTopic")
        sub_url = tp.subscribe_queue_endpoint("ml-test-sub", "1667348596157055", "TestMachineLearningQueue")
        self.assertIsNot(sub_url, "")

    def test_unsubscribe(self):
        mm.setup_message(
            "http://1667348596157055.mns.cn-hangzhou.aliyuncs.com",
            "sulgyCs1lNU420ra",
            "QbXX86FSRUNHEMyEGXfHznJ00xeeop")
        tp = mm.get_topic("TestTopic")
        result = tp.unsubscribe("ml-test-sub")
        self.assertTrue(result)
