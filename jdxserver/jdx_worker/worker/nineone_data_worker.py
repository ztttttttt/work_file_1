import json
import re


class NineOneDataWorker(object):
    def __init__(self, mysql_client):
        self.mysql_ml = mysql_client

    def run(self, derivate_data):
        mobile_list = self.get_mobile_list(derivate_data.get('X_User_addressBook'))
        if not mobile_list:
            return {}
        # 查mysql
        address_list = self.get_address_list(mobile_list)
        city_list = [x.get('city') for x in address_list]
        province_list = [x.get('province') for x in address_list]

        derivatives = {
            'X_User_addressBook_contactCity_cnt': len(set(city_list)),
            'X_User_addressBook_contactProvince_cnt': len(set(province_list))
        }
        return derivatives

    def get_address_list(self, mobile_list, table='mobile_phone_area'):
        mobile_prefix_list = [mobile[:7] for mobile in mobile_list]
        mobile_prefixs = tuple(set(mobile_prefix_list))
        if len(mobile_prefixs) == 1:
            sql = """SELECT prefix, province, city, service_provider FROM {} WHERE prefix='{}';""".format(table, mobile_prefixs[0])
        else:
            sql = """SELECT prefix, province, city, service_provider FROM {} WHERE prefix in {};""".format(table, mobile_prefixs)
        address_list = self.mysql_ml.query(sql)
        return address_list

    def get_mobile_list(self, address_book):
        mobile_list = []
        if address_book and address_book != '[]':
            address_book = json.loads(address_book)
            if type(address_book) is list:
                address_book = address_book[0].get('contents')
            else:
                address_book = address_book.get('contents')
            for c in address_book:
                try:
                    mobile = c['mobile']
                    if mobile:
                        if mobile.startswith("+86"):
                            mobile = mobile[3:]

                        match_res = re.compile(r"^1(\d){10}$").match(mobile)  # 电话号码基本特征
                        if match_res:
                            mobile_list.append(mobile)
                except:
                    continue
        return mobile_list
