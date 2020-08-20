from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
import json
import numpy as np
import pymongo
from mlx_database.dbdata import DBData
from mlx_database import mysql, mongo


class GraphUserData(DBData):
    def __init__(self, client=None, config=None):
        super(GraphUserData, self).__init__(table="ml_graph_users", client=client)

    def insert(self, user_id, app_id, product_name, mobile, id_number, longitude, latitude, device_id,
               basestation_id, mobile_os, bssid, principal, application_time):
        stmt = "insert into {}(user_id, app_id, product_name, mobile, id_number, longitude, latitude,\
                device_id, basestation_id, mobile_os, bssid, principal, application_time)\
                values('{}', '{}', '{}', '{}', '{}', {}, {}, '{}', '{}', '{}', '{}', {}, '{}')".format(
            self.table, user_id, app_id, product_name, mobile, id_number, longitude, latitude,
            device_id, basestation_id, mobile_os, bssid, principal, application_time
        )
        return self.client.update(stmt) == 1

    def query_edge_by_field_userid(self, field, field_value, user_id):
        if isinstance(field_value, (int, float)):
            field_value = "{}".format(field_value)
        else:
            field_value = "'{}'".format(field_value)
        stmt = "select distinct '{}' source,user_id target " \
               "from {} where {} = {} and user_id <>'{}'"
        stmt = stmt.format(user_id, self.table, field, field_value, user_id)
        return self.client.query(stmt)

    def query_edge_by_gps(self, user_id, longitude, latitude, long_scope=2.0, lat_scope=10.0, delay=1):
        stmt = "select '{}' source, user_id target, longitude, latitude from {} \
               where application_time > '{}' and longitude between {} and {} and \
               latitude between {} and {} and longitude is not null and latitude is not null \
               and user_id <> '{}'"
        stmt = stmt.format(user_id, self.table, str(datetime.now() - timedelta(delay)), longitude - long_scope,
                           longitude + long_scope,
                           latitude - lat_scope, latitude + lat_scope, user_id)
        return self.client.query(stmt)

    def query_edge_by_mobiles(self, mobiles, user_id, max_len=1000):
        if len(mobiles) > max_len:
            prev_i = 0
            result = []
            for i in range(max_len, len(mobiles), max_len):
                batch_mobiles = mobiles[prev_i: i]
                prev_i = i
                result.extend(self.query_edge_by_mobiles_batch(batch_mobiles, user_id))
            if i < len(mobiles) - 1:
                result.extend(self.query_edge_by_mobiles_batch(batch_mobiles, user_id)[i:])
            return result
        else:
            return self.query_edge_by_mobiles_batch(mobiles, user_id)

    def query_edge_by_mobiles_batch(self, mobiles, user_id):
        mobiles = str(tuple(mobiles)).replace(',)', ')')
        stmt = "select distinct '{}' source ,user_id target from {} " \
               "where user_id<>'{}' and mobile in {}"
        stmt = stmt.format(user_id, self.table, user_id, mobiles)
        return self.client.query(stmt)

    def query_appear_times_by_field(self, field, field_value, interval=1):
        stmt = "select count(distinct user_id) appear_times from {} where " \
               "{} = '{}' and " \
               "application_time>date_add(current_timestamp(), INTERVAL -{} DAY)"
        stmt = stmt.format(self.table, field, field_value, interval)
        ret = self.client.query(stmt)
        return ret[0]['appear_times'] if ret else 0

    def query_alluser_info(self, start_date='1970-01-01'):
        stmt = """SELECT DISTINCT user_id,
                create_time as last_application_time,
                product_name,
                id_number,
                principal + 0E0 principal
                FROM {table} 
                WHERE id_number != 'None'
                AND create_time > '{start_date}'
                GROUP BY user_id
                ORDER BY create_time DESC;
        """
        stmt = stmt.format(table=self.table, start_date=start_date)
        return self.client.query(stmt)


class GraphData(DBData, metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, client=None, table=None):
        super(GraphData, self).__init__(table=table, client=client)
        self.field = ''
        self.field_type = 'str'

    def insert(self, user_id1, user_id2, field_value):
        stmt = "insert into {}(user_id1, user_id2, {}) values('{}', '{}', {})"
        if self.field_type == 'numeric':
            field_value = "{}".format(field_value)
        else:
            field_value = "'{}'".format(field_value)
        stmt = stmt.format(self.table, self.field, user_id1, user_id2, field_value)
        return self.client.update(stmt) == 1

    def insert_batch(self, values):
        stmt = "insert into {}(user_id1, user_id2, {}) values {}"
        stmt = stmt.format(self.table, self.field, ",".join(['{}'.format(s) for s in values]))
        return self.client.update(stmt) == len(values)

    def query_edges(self, type_, create_start_date="1970-01-01"):
        stmt = "select distinct user_id1 source,user_id2 target, '{}' type " \
               "from {} " \
               "where user_id1<>user_id2 and LENGTH(user_id1) > 10 and "\
               "LENGTH(user_id2) > 10 and create_time > '{}'"
        stmt = stmt.format(type_, self.table, create_start_date)
        return self.client.query(stmt)


class GraphGPSData(GraphData):

    def __init__(self, client=None, config=None):
        super(GraphGPSData, self).__init__(table="ml_graph_gps", client=client)
        self.field = 'gps_distance'
        self.field_type = 'numeric'


class GraphBaseStationData(GraphData):

    def __init__(self, client=None, config=None):
        super(GraphBaseStationData, self).__init__(table="ml_graph_basestation", client=client)
        self.field = 'basestation_id'
        self.field_type = 'str'


class GraphBssidData(GraphData):

    def __init__(self, client=None, config=None):
        super(GraphBssidData, self).__init__(table="ml_graph_bssid", client=client)
        self.field = 'bssid'
        self.field_type = 'str'


class GraphDeviceIdData(GraphData):

    def __init__(self, client=None, config=None):
        super(GraphDeviceIdData, self).__init__(table="ml_graph_deviceid", client=client)
        self.field = 'device_id'
        self.field_type = 'str'


class GraphContactBookData(GraphData):

    def __init__(self, client=None, config=None):
        super(GraphContactBookData, self).__init__(table="ml_graph_contactbook", client=client)
        self.field = None
        self.field_type = None

    def insert(self, user_id1, user_id2):
        stmt = "insert into {}(user_id1, user_id2) values('{}', '{}')"
        stmt = stmt.format(self.table, user_id1, user_id2)
        return self.client.update(stmt) == 1

    def insert_batch(self, values):
        stmt = "insert into {}(user_id1, user_id2) values {}"
        stmt = stmt.format(self.table, ",".join(['{}'.format(s) for s in values]))
        return self.client.update(stmt) == len(values)
