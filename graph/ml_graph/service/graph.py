import logging
import os
import threading
from mlx_utility import cal_gps_distance
from datetime import datetime, timedelta
import time
from mlx_graph.mlx_graph import MlxGraph
import numpy as np
from collections import Counter
from ml_graph.dal import graph_data


class EdgeCalculate:
    def __init__(self, gps_data, bs_data, bssid_data, deviceid_data, user_data, contactbook_data):
        self.gps_data = gps_data
        self.bs_data = bs_data
        self.bssid_data = bssid_data
        self.deviceid_data = deviceid_data
        self.user_data = user_data
        self.contactbook_data = contactbook_data

    def calcu_edge(self, edge_type, userid, user_infos, **params):
        if edge_type == 'gps':
            return self.calcu_gps_edge(userid, user_infos.get('longitude'), user_infos.get('latitude'), **params)
        elif edge_type == 'device_id':
            return self.calcu_deviceid_edge(userid, user_infos.get('device_id'))
        elif edge_type == 'bssid':
            return self.calcu_bssid_edge(userid, user_infos.get('bssid'))
        elif edge_type == 'contactbook':
            return self.calcu_contactbook_edge(userid, user_infos.get('contactbook'))
        elif edge_type == 'base_station':
            return self.calcu_basestation_edge(userid, user_infos.get('base_station'))

    def calcu_contactbook_edge(self, userid, contactbook):
        edge_list = []
        if contactbook:
            mobiles = [x for x in contactbook]
            query_result = self.user_data.query_edge_by_mobiles(mobiles, userid)
            if query_result:
                edge_list += query_result
                edge_tuples = [tuple(x.values()) for x in query_result]
                self.contactbook_data.insert_batch(edge_tuples)

        for ed in edge_list:
            ed['type'] = 'contactbook'
        return edge_list

    def calcu_basestation_edge(self, userid, base_station):
        edge_list = []
        if base_station:
            query_rlt = self.user_data.query_edge_by_field_userid('basestation_id', base_station, userid)
            if query_rlt:
                edge_list += query_rlt
                edge_tuples = list(map(lambda x: tuple(x.values()).__add__((base_station,)), query_rlt))
                self.bs_data.insert_batch(edge_tuples)

        for ed in edge_list:
            ed['type'] = 'basestation'
        return edge_list

    def calcu_bssid_edge(self, userid, bssid):
        edge_list = []
        if bssid:
            query_rlt = self.user_data.query_edge_by_field_userid('bssid', bssid, userid)
            if query_rlt:
                edge_list += query_rlt
                edge_tuples = list(map(lambda x: tuple(x.values()).__add__((bssid,)), query_rlt))
                self.bssid_data.insert_batch(edge_tuples)

        for ed in edge_list:
            ed['type'] = 'bssid'
        return edge_list

    def calcu_deviceid_edge(self, userid, device_id):
        edge_list = []
        if device_id:
            query_rlt = self.user_data.query_edge_by_field_userid('device_id', device_id, userid)
            if query_rlt:
                edge_list += query_rlt
                edge_tuples = list(map(lambda x: tuple(x.values()).__add__((device_id,)), query_rlt))
                self.deviceid_data.insert_batch(edge_tuples)

        for ed in edge_list:
            ed['type'] = 'device'
        return edge_list

    def calcu_gps_edge(self, userid, longitude, latitude, **params):
        days = params.get('days', 2)
        distance = params.get('distance', 100)
        application_time = params.get('application_time', datetime.now())
        edge_list = []
        if longitude and latitude:
            query_rlt = self.user_data.query_edge_by_gps(userid, longitude, latitude)
            if query_rlt:
                for record in query_rlt:
                    record['gps_distance'] = cal_gps_distance.cal_gps_distance(longitude, latitude, record['longitude'],
                                                                               record['latitude'])

                neighs = [{'source': q['source'], 'target': q['target'], 'gps_distance': q['gps_distance']}
                          for q in query_rlt if q['gps_distance'] <= distance]

                if neighs:
                    edge_tuples = [(nei['source'], nei['target'], round(nei['gps_distance'], 6)) for nei in neighs]
                    self.gps_data.insert_batch(edge_tuples)
                    edge_list = neighs

        for ed in edge_list:
            ed['type'] = 'gps'
        return edge_list


class GraphFeatures:
    def __init__(self, id_number, user_data, **user_info_dict):
        self.user_data = user_data
        self.id_number = id_number
        self.user_info_dict = user_info_dict
        self.init_graph_features()

    def init_graph_features(self):
        self.degree = 0
        self.avg_principal = float(self.user_info_dict.get('principal'))
        self.recent_3days_usercnt = 1
        self.eigen_vector = 1
        self.page_rank = 1
        self.betweenness = 0
        self.subgraph_size = 1
        self.edge_count = 0
        self.deviceid_appear_times = 1 if self.user_info_dict.get('device_id') else None
        self.basestation_appear_times = 1 if self.user_info_dict.get('base_station') else None
        self.female_cnt = 1 if int(self.id_number[-2]) % 2 == 0 else 0
        self.avg_age = datetime.now().year - int(self.id_number[6:10])
        self.main_age = datetime.now().year - int(self.id_number[6:10])
        self.main_area_code = int(self.id_number[:6])
        self.gpsgraph_size_in1day = None
        self.devicegraph_size_in1day = None
        self.bsgraph_size_in1day = None
        self.closeness = None
        self.neighbors_mintime_interval = None

    def cal_graph_features(self, graph, userid, device_id, base_station):
        subg = graph.find_subgraph(userid)
        subg_gps = graph.find_subgraph(userid, 'gps')
        subg_device = graph.find_subgraph(userid, 'device')
        subg_basestation = graph.find_subgraph(userid, 'basestation')
        v = subg.vs.find(userid)
        v_seq = subg.vs
        neiv_seq = subg.vs(set(subg.neighbors(v)))
        last_day = datetime.now() - timedelta(1)

        if subg_gps:
            self.gpsgraph_size_in1day = len(
                subg_gps.vs(last_application_time_ne=None)(last_application_time_gt=last_day))
        if subg_device:
            self.devicegraph_size_in1day = len(
                subg_device.vs(last_application_time_ne=None)(last_application_time_gt=last_day))
        if subg_basestation:
            self.bsgraph_size_in1day = len(
                subg_basestation.vs(last_application_time_ne=None)(last_application_time_gt=last_day))

        self.subgraph_size = subg.vcount()
        self.closeness = subg.closeness(v)
        self.eigen_vector = subg.eigenvector_centrality(v)[0]
        self.page_rank = subg.pagerank(v)
        self.betweenness = subg.betweenness(v)
        self.degree = v.degree()
        self.edge_count = subg.ecount()
        self.female_cnt = len(v_seq(idcard_gender_eq=0))
        self.avg_principal = np.mean(v_seq(principal_ne=None)['principal'])
        self.main_area_code = int(Counter(v_seq(area_code_ne=None)['area_code']).most_common()[0][0])
        self.recent_3days_usercnt = len(
            v_seq(last_application_time_ne=None)(last_application_time_gt=datetime.now() - timedelta(3)))
        self.avg_age = np.mean(v_seq(idcard_age_ne=None)['idcard_age'])
        self.main_age = int(Counter(v_seq(idcard_age_ne=None)['idcard_age']).most_common()[0][0])

        if neiv_seq(last_application_time_ne=None)['last_application_time']:
            self.neighbors_mintime_interval = (datetime.now() - max(
                neiv_seq(last_application_time_ne=None)['last_application_time'])).seconds / 60.0

        if device_id:
            self.deviceid_appear_times = self.user_data.query_appear_times_by_field('device_id', device_id)

        if base_station:
            self.basestation_appear_times = self.user_data.query_appear_times_by_field('basestation_id', base_station)

    def todict(self):
        return {
            'X_Graph_Degree': self.degree,
            'X_Graph_AvgPrincipal': self.avg_principal,
            'X_Graph_Recent3DaysUserCnt': self.recent_3days_usercnt,
            'X_Graph_Eigenvector': self.eigen_vector,
            'X_Graph_PageRank': self.page_rank,
            'X_Graph_Betweenness': self.betweenness,
            'X_Graph_SubgraphSize': self.subgraph_size,
            'X_Graph_EdgesCount': self.edge_count,
            'X_Graph_DeviceIdAppearTimes': self.deviceid_appear_times,
            'X_Graph_DeviceBaseStationAppearTimes': self.basestation_appear_times,
            'X_Graph_FemaleCnt': self.female_cnt,
            'X_Graph_AvgAge': self.avg_age,
            'X_Graph_MainAge': self.main_age,
            'X_Graph_MainAreacode': self.main_area_code,
            'X_Graph_GpsGraphSizeIn1Day': self.gpsgraph_size_in1day,
            'X_Graph_DeviceGraphSizeIn1Day': self.devicegraph_size_in1day,
            'X_Graph_BaseStationGraphSizeIn1Day': self.bsgraph_size_in1day,
            'X_Graph_Closeness': self.closeness,
            'X_Graph_NeighborsMinTimeInterval': self.neighbors_mintime_interval
        }


class MlGraph(MlxGraph):
    def __init__(self, graph_db_client, graph_min_vertices_cnt, graph_uri):
        super(MlGraph, self).__init__()
        self.graph_db_client = graph_db_client
        self.graph_min_vertices_cnt = graph_min_vertices_cnt
        self.uri = graph_uri
        self.graph = MlxGraph()
        self.gps_data = graph_data.GraphGPSData(client=graph_db_client)
        self.bs_data = graph_data.GraphBaseStationData(client=graph_db_client)
        self.bssid_data = graph_data.GraphBssidData(client=graph_db_client)
        self.deviceid_data = graph_data.GraphDeviceIdData(client=graph_db_client)
        self.user_data = graph_data.GraphUserData(client=graph_db_client)
        self.contactbook_data = graph_data.GraphContactBookData(client=graph_db_client)
        self.edge_caculator = EdgeCalculate(self.gps_data, self.bs_data, self.bssid_data, self.deviceid_data,
                                            self.user_data, self.contactbook_data)
        self.edge_types = ["device_id", "base_station", "bssid", "gps", "contactbook"]

    def initial_graph(self):
        logging.info('start initial graph ...')
        t1 = time.time()
        vertices = self._get_all_vertices()
        logging.info("TEMP: vertices done, len(vertices) : {}".format(len(vertices)))
        edges = self._get_all_edges()
        logging.info("TEMP: edges done, len(edges) : {}".format(len(edges)))
        if len(vertices) < self.graph_min_vertices_cnt:
            vcount, ecount = self.load_graph(self.uri)
        else:
            vcount, ecount = self.build(vertices, edges)
        logging.info("vertices size : {}".format(vcount))
        logging.info("edges size : {}".format(ecount))
        logging.info("it takes {} s to create graph!".format(time.time() - t1))
        self.save_graph(self.uri)

    def _get_all_vertices(self):
        # 加载最近90天数据就行
        now = datetime.now()
        day_90 = now - timedelta(days=90)
        query_start_date = day_90.strftime("%Y-%m-%d")
        logging.info("init: query vertices start:{}, end:{}".format(query_start_date, "now"))
        users_info = self.user_data.query_alluser_info(query_start_date)
        vertices = []
        for user_info in users_info:
            vetex = self._construct_vetex_attrs(**user_info)
            vetex['name'] = vetex['user_id']
            vertices.append(vetex)
        return vertices

    def _get_all_edges(self):
        edges = []
        for et in self.edge_types:
            data_client = None
            if et == 'contactbook':
                data_client = self.contactbook_data
            elif et == 'gps':
                data_client = self.gps_data
            elif et == 'device_id':
                data_client = self.deviceid_data
            elif et == 'base_station':
                data_client = self.bs_data
            elif et == 'bssid':
                data_client = self.bssid_data
            else:
                logging.error('no such edge type : {}'.format(et))
            # 查询最近90天即可
            now = datetime.now()
            day_90 = now - timedelta(days=90)
            query_start_date = day_90.strftime("%Y-%m-%d")

            if data_client:
                logging.info("init: query edges start:{}, end:{}".format(query_start_date, "now"))
                edges.extend(data_client.query_edges(et, query_start_date))

        return edges

    def _save_user_data(self, user_id, app_id, id_number, product_name, **params):
        nonnull_params = {k: v for k, v in params.items() if v is not None}
        self.user_data.insert(user_id, app_id, product_name, nonnull_params.get('mobile', ''), id_number,
                              nonnull_params.get('longitude', 0),
                              nonnull_params.get('latitude', 0), nonnull_params.get('device_id', ''),
                              nonnull_params.get('base_station', ''),
                              nonnull_params.get('mobile_os', ''), nonnull_params.get('bssid', ''),
                              nonnull_params.get('principal', 0),
                              nonnull_params.get('application_time', datetime.now()))

    def _construct_vetex_attrs(self, user_id, product_name, id_number, principal, last_application_time):
        return {'idcard_gender': int(id_number[-2]) % 2,
                'principal': float(principal),
                'area_code': int(id_number[:6]),
                'idcard_age': datetime.now().year - int(id_number[6:10]),
                'product_name': product_name,
                'user_id': user_id,
                'last_application_time': last_application_time}

    def _update_vertex(self, userid, **v_attrs):
        if userid in self.g.vs['name']:
            v = self.find_vertex(userid)
            v.update_attributes(**v_attrs)
        else:
            self.add_vertex(userid, **v_attrs)

    def _add_edges(self, edge_list):
        [self.add_edge(**ed) for ed in edge_list if not self.edge_exists(ed['source'], ed['target'], ed['type'])]

    def cal_graph_features(self, app_id, product_name, user_id, id_number, edge_types, **params):
        t1 = time.time()
        # insert user data
        self._save_user_data(user_id, app_id, id_number, product_name, **params)
        graph_features = GraphFeatures(id_number, self.user_data, **params)
        if isinstance(params['application_time'], (str)):
            try:
                params['application_time'] = datetime.strptime(params['application_time'], '%Y-%m-%d %H:%M:%S.%f')
            except Exception:
                logging.error('Error datetype: {}'.format(params['application_time']))
                params['application_time'] = datetime.now()
        vetex_attrs = self._construct_vetex_attrs(user_id, product_name, id_number, params['principal'],
                                                  params['application_time'])

        edge_list = self._get_edges(user_id, params, edge_types)

        self._update_vertex(user_id, **vetex_attrs)
        self._add_edges(edge_list)

        graph_features.cal_graph_features(self, user_id, params.get('device_id'), params.get('base_station'))
        # 开一个线程,超时就返回默认值
        # t = threading.Thread(target=self.update_graph_and_cal_features,
        #                      args=(graph_features, app_id, user_id, edge_list, params, vetex_attrs))
        # t.setDaemon(True)
        # t.start()
        # t.join(300)

        logging.info("it takes {} s to cal graph features for app_id: {}!".format(time.time() - t1, app_id))
        return graph_features.todict()

    def update_graph_and_cal_features(self, graph_features, app_id, user_id, edge_list, vetex_attrs, params):
        t1 = time.time()
        # start update graph
        self._update_vertex(user_id, **vetex_attrs)  # 更新顶点
        self._add_edges(edge_list)  # 添加新边

        logging.info("it takes {} s to insert user data to graph by app_id: {}!".format(time.time() - t1, app_id))
        graph_features.cal_graph_features(self, user_id, params.get('device_id'), params.get('base_station'))

    def _get_edges(self, user_id, user_infos, edge_types):
        edge_list = []
        params = {
            "days": 2,
            "distance": 100,
            "application_time": user_infos.get("application_time", datetime.now())
        }
        for et in edge_types:
            edge_list += self.edge_caculator.calcu_edge(et, user_id, user_infos, **params)
        return edge_list
