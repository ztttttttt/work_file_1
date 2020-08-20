# -*- coding: UTF-8 -*-
from DBUtils.PooledDB import PooledDB
from mlx_database.database import Database
import pymssql
import logging


class SqlServer(Database):

    def __init__(self, config):
        Database.__init__(self, config)

    def get_connection(self):
        if self._pool is None:
            self._pool = PooledDB(creator=pymssql, mincached=1, maxcached=5, host=self.config['server'],
                                  port=self.config['port'],
                                  user=self.config['user'], password=self.config['password'], database=self.database,
                                  charset='utf8', setsession=['SET IMPLICIT_TRANSACTIONS OFF'])
        return self._pool.connection()

    def _get_cursor(self):
        conn = self.get_connection()
        cursor = conn.cursor(as_dict=True)
        return cursor

    def query(self, sql, args=None, retry=3):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql: 查询SQL，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param args: 查询SQL中的变量
        @param retry: 尝试重连次数
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        try:
            cursor = self._get_cursor()
            cursor.execute(sql, args)
            result = cursor.fetchall()
            return result
        except Exception as ex:
            logging.exception("sqlserver db query error!")
            retry -= 1
            if retry <= 0:
                raise ex
            else:
                self._pool = None
                return self.query(sql, args, retry)

    def update(self, sql, args=None, retry=3):
        try:
            cursor = self._get_cursor()
            count = cursor.execute(sql, args)
            cursor.execute("COMMIT TRANSACTION")
            return count
        except Exception as ex:
            logging.exception("sqlserver db update error!")
            retry -= 1
            if retry <= 0:
                raise ex
            else:
                self._pool = None
                return self.update(sql, args, retry)
