from math import *


def cal_gps_distance(lon1, lat1, lon2, lat2):
    """
    :param lon1: decimal longitude,float
    :param lat1: decimal latitude,float
    :param lon2: decimal longitude,float
    :param lat2: decimal latitude,float
    :return: distance between two point ,meters, float
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    dis = 2 * asin(sqrt(a)) * 6371 * 1000
    return dis
