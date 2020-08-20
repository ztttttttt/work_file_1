import requests
import logging


def get_json(uri, params=None, **kwargs):
    logging.info("send http request. method: GET, uri: {}, params: {}, kwargs: {}".format(uri, params, kwargs))
    return requests.get(uri, params, **kwargs).json()


def post_data(uri, data, **kwargs):
    logging.info("send http request. method: POST, uri: {}, data: {}, kwargs: {}".format(uri, data, kwargs))
    return requests.post(uri, data=data, **kwargs).json()
