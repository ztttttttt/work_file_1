# -*- coding: UTF-8 -*-


class Database:
    def __init__(self, host, port, user, pw, db):
        self._host = host
        self._port = port
        self._user = user
        self._pw = pw
        self._db = db
        self._pool = None

    def _init_pool(self):
        pass

    def get_conn(self):
        pass

    def update(self, sql, args=None, retry=3):
        pass

    def query(self, sql, args=None, retry=3):
        pass

    def query_one(self, sql, args=None, retry=3):
        pass
