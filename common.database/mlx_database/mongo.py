from pymongo import MongoClient
from mlx_database.database import Database
import logging


class Mongo(Database):

    def __init__(self, host, user, pw, db, **kwargs):
        super().__init__(host, None, user, pw, db)  # port should be in 'host'
        self._kwargs = kwargs
        self.get_conn()

    def _init_pool(self):
        uri = "mongodb://{0}:{1}@{2}".format(self._user, self._pw, self._host)
        self._kwargs.setdefault('readPreference', 'secondaryPreferred')
        self._pool = MongoClient(host=uri, **self._kwargs)

    def get_conn(self):
        if not self._pool:
            self._init_pool()
        return self._pool

    def get_collection(self, collection, retry=3):
        try:
            return self.get_conn()[self._db][collection]
        except Exception as ex:
            logging.exception("mongo get_collection error!")
            retry -= 1
            if retry <= 0:
                raise ex
            else:
                self._init_pool()
                return self.get_collection(collection, retry)
