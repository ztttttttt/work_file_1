# -*- coding: UTF-8 -*-
from mlx_database import database
from pyhive import hive
import pandas as pd


class Hive(database.Database):

    def __init__(self, host, port, user, pw, db=None, **kwargs):
        super().__init__(host, port, user, pw, db)
        self._kwargs = kwargs

    def get_connection(self):
        self._kwargs.setdefault('auth', 'CUSTOM')
        conn = hive.connect(host=self._host, port=self._port, username=self._user, password=self._pw, **self._kwargs)
        return conn

    def _get_cursor(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        return cursor, conn

    def query(self, sql, args=None, retry=3):
        cursor, conn = self._get_cursor()
        cursor.execute(sql)
        columns = [x[0] for x in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return pd.DataFrame(data=data, columns=columns)

    def update(self, sql, args=None, retry=3):
        cursor, conn = self._get_cursor()
        cursor.execute(sql)
        cursor.close()
        conn.close()
