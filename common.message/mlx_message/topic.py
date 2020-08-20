import mns.topic as mns_topic
import mns.subscription as mns_sub
import mns.mns_exception as mns_exception
import mns.mns_common as mns_common
import logging
import base64


class Topic(object):
    def __init__(self, topic):
        if not isinstance(topic, mns_topic.Topic):
            raise TypeError("Sorry! It has to be an instance of mns topic for the moment.")
        self.topic = topic

    def publish_message(self, message_body):
        try:
            message_body_b64 = base64.b64encode(message_body)
            message = mns_topic.TopicMessage(message_body_b64)
            published_msg = self.topic.publish_message(message)
            logging.info("Publish topic message succeed. topic: %s, "
                         "message body: %s, message id: %s, message body MD5: %s",
                         self.topic.topic_name, message_body, published_msg.message_id, published_msg.message_body_md5)
            return True
        except mns_exception.MNSExceptionBase:
            logging.exception("Publish topic message fail! topic: %s, message body: %s",
                              self.topic.topic_name, message_body)
            return False

    def subscribe_queue_endpoint(self, sub_name, account_id, queue_name, region="cn-hangzhou"):
        try:
            subscription = self.topic.get_subscription(sub_name)
            queue_endpoint = mns_common.TopicHelper.generate_queue_endpoint(region, account_id, queue_name)
            subscription_meta = mns_topic.SubscriptionMeta(
                endpoint=queue_endpoint,
                notify_strategy=mns_sub.SubscriptionNotifyStrategy.BACKOFF,
                notify_content_format=mns_sub.SubscriptionNotifyContentFormat.SIMPLIFIED)
            sub_url = subscription.subscribe(subscription_meta)
            logging.info("Subscribe topic succeed. topic %s, subscription: %s, endpoint: %s, subscription url: %s",
                         self.topic.topic_name, sub_name, queue_endpoint, sub_url)
            return sub_url
        except mns_exception.MNSExceptionBase:
            logging.exception("Subscribe topic fail! topic: %s, subscription: %s", self.topic.topic_name, sub_name)
            return ""

    def unsubscribe(self, sub_name):
        try:
            subscription = self.topic.get_subscription(sub_name)
            subscription.unsubscribe()
            logging.info("Unsubscribe topic succeed. topic: %s, subscription: %s", self.topic.topic_name, sub_name)
            return True
        except mns_exception.MNSExceptionBase:
            logging.exception("Unsubscribe topic fail! topic: %s, subscription: %s", self.topic.topic_name, sub_name)
            return False
