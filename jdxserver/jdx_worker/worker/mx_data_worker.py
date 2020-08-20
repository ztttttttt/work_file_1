from datetime import datetime
import json
import logging
import re
import traceback
from mlx_utility import resolve_data


class MxDataWorker(object):
    def __init__(self):
        self.prefix = 'X_MX_'
        self.mobile_service_prefix = "X_MobileService_"  # compatible with JXL
        self.estimated_month_re = re.compile(r'使用(:?\d+)个月')
        self.mobile_online_map = {
            0: '01',
            3: '02',
            6: '03',
            12: '04',
            24: '05'
        }
        self.service_provider_map = {
            'CHINA_MOBILE': "移动",
            'CHINA_TELECOM': "电信",
            'CHINA_UNICOM': "联通"
        }

    def _get_data_by_type(self, der_datas, type_):
        data = None
        try:
            if type_ == 'report':
                data = json.loads(der_datas.get('X_MX_RawReport'))
            else:
                data = json.loads(der_datas.get('X_MX_OriginReport'))
        except TypeError:
            pass  # if data der_datas.get is none
        except BaseException:
            logging.warning("MX data json.loads error. {}\n {}".format(type_, traceback.print_exc()))
        return data

    def run(self, derivate_data):
        report = self._get_data_by_type(derivate_data, 'report')
        raw = self._get_data_by_type(derivate_data, 'origin')
        derivatives = {}
        if raw:
            derivatives.update(resolve_data.resolve_data(raw, self.prefix + 'raw_data_'))
            derivatives.update(self.__extract_raw_data_derivatives(raw))
        if report:
            derivatives.update(resolve_data.resolve_data(report, self.prefix + 'raw_report_'))
            if report.get('contact_list'):
                derivatives.update(self.__extract_contact_list_derivatives(report['contact_list']))
            if report.get('contact_region'):
                derivatives.update(self.__extract_contact_region_derivatives(report['contact_region']))
            if report.get('user_info_check'):
                derivatives.update(self.__extract_user_info_check_derivatives(report['user_info_check']))
            if report.get('cell_behavior'):
                derivatives.update(self.__extract_cell_behavior_derivatives(report['cell_behavior']))
            if report.get('behavior_check'):
                derivatives.update(self.__extract_behavior_check_derivatives(report['behavior_check']))

        return derivatives

    def __extract_contact_list_derivatives(self, contact_list):
        derivatives = {}
        derivatives[self.mobile_service_prefix + 'ContactList_Cnt'] = len(contact_list)
        call_in_cnt = int(sum([c['call_in_cnt'] for c in contact_list]))
        derivatives[self.mobile_service_prefix + 'ContactList_CallInCnt'] = call_in_cnt
        call_out_cnt = int(sum([c['call_out_cnt'] for c in contact_list]))
        derivatives[self.mobile_service_prefix + 'ContactList_CallOutCnt'] = call_out_cnt
        call_in_len = int(sum([c['call_in_len'] for c in contact_list])) / 60  # to minutes
        derivatives[self.mobile_service_prefix + 'ContactList_CallInLen'] = call_in_len
        call_out_len = int(sum([c['call_out_len'] for c in contact_list])) / 60  # to minutes
        derivatives[self.mobile_service_prefix + 'ContactList_CallOutLen'] = call_out_len
        derivatives[self.mobile_service_prefix + 'ContactList_AvgCallOutLen'] = \
            call_out_len / call_out_cnt if call_out_cnt > 0 else 0
        derivatives[self.mobile_service_prefix + 'ContactList_AvgCallInLen'] = \
            call_in_len / call_in_cnt if call_in_cnt > 0 else 0

        contact_list_sorted = sorted(contact_list, key=lambda c: c['call_cnt'], reverse=True)
        top5_contact_call_cnt = int(sum([c['call_cnt'] for c in contact_list_sorted[:5]]))
        derivatives[self.mobile_service_prefix + 'ContactList_Top5ContactCallCnt'] = top5_contact_call_cnt
        top10_contact_call_cnt = int(sum([c['call_cnt'] for c in contact_list_sorted[:10]]))
        derivatives[self.mobile_service_prefix + 'ContactList_Top10ContactCallCnt'] = top10_contact_call_cnt

        top1_contact = contact_list_sorted[0]
        rc = re.compile(r'1\d{10}', re.IGNORECASE)
        is_top1_contact_mobile = 1 if rc.match(top1_contact['phone_num']) else 0
        derivatives[self.mobile_service_prefix + 'ContactList_IsTop1ContactMobile'] = is_top1_contact_mobile

        # contact list for graph
        contact_list_for_graph = []
        for c in contact_list:
            contact = {
                'phone_num': c['phone_num'],
                'call_in_cnt': c['call_in_cnt'],
                'call_out_cnt': c['call_out_cnt'],
                'call_in_len': c['call_in_len'],
                'call_out_len': c['call_out_len']
            }
            contact_list_for_graph.append(contact)
        derivatives[self.mobile_service_prefix + 'ContactList'] = contact_list_for_graph
        return derivatives

    def __extract_contact_region_derivatives(self, contact_region):
        derivatives = {}
        derivatives[self.mobile_service_prefix + 'ContactRegion_Cnt'] = len(contact_region)
        region_uniq_num_cnt = int(sum([c['region_uniq_num_cnt'] for c in contact_region]))
        derivatives[self.mobile_service_prefix + 'ContactRegion_RegionUniqNumCnt'] = region_uniq_num_cnt

        province_mainland = ['上海', '江苏', '北京', '全国', '安徽', '广东', '浙江', '陕西', '云南',
                             '四川', '宁夏', '福建', '河南', '未知', '天津', '江西', '山东', '河北',
                             '湖北', '湖南', '辽宁', '运营', '重庆', '青海', '黑龙', '新疆', '广西',
                             '甘肃', '山西', '内蒙', '吉林', '贵州', '西藏', '海南']
        total_call_in_cnt = sum(
            [c['region_call_in_cnt'] for c in contact_region if c['region_loc'][:2] not in province_mainland])
        total_call_out_cnt = sum(
            [c['region_call_out_cnt'] for c in contact_region if c['region_loc'][:2] not in province_mainland])
        derivatives[self.mobile_service_prefix + 'ContactRegion_FremdnessNetinCnt'] = \
            total_call_in_cnt - total_call_out_cnt

        total_call_in_len = sum(
            [c['region_call_in_time'] for c in contact_region if c['region_loc'][:2] not in province_mainland])
        derivatives[self.mobile_service_prefix + 'ContactRegion_FremdnessTimeCnt'] = total_call_in_len

        return derivatives

    def __extract_user_info_check_derivatives(self, user_info_check):
        derivatives = {}

        check_search_info = user_info_check['check_search_info']
        phone_with_other_names_cnt = len(check_search_info['phone_with_other_names'])
        derivatives[self.mobile_service_prefix + 'UserInfoCheck_PhoneWithOtherNamesCnt'] = phone_with_other_names_cnt
        phone_with_other_idcards_cnt = len(check_search_info['phone_with_other_idcards'])
        derivatives[
            self.mobile_service_prefix + 'UserInfoCheck_PhoneWithOtherIdcardsCnt'] = phone_with_other_idcards_cnt
        register_org_cnt = self.__get_number_value_from_data_dict(check_search_info, 'register_org_cnt')
        derivatives[self.mobile_service_prefix + 'UserInfoCheck_RegisterOrgCnt'] = register_org_cnt

        check_black_info = user_info_check['check_black_info']
        phone_gray_score = self.__get_number_value_from_data_dict(check_black_info, 'phone_gray_score')
        derivatives[self.mobile_service_prefix + 'UserInfoCheck_PhoneGrayScore'] = phone_gray_score
        contacts_class1_blacklist_cnt = self.__get_number_value_from_data_dict(check_black_info,
                                                                               'contacts_class1_blacklist_cnt')
        derivatives[
            self.mobile_service_prefix + 'UserInfoCheck_ContactsClass1BlacklistCnt'] = contacts_class1_blacklist_cnt
        contacts_class2_blacklist_cnt = self.__get_number_value_from_data_dict(check_black_info,
                                                                               'contacts_class2_blacklist_cnt')
        derivatives[
            self.mobile_service_prefix + 'UserInfoCheck_ContactsClass2BlacklistCnt'] = contacts_class2_blacklist_cnt
        contacts_class1_cnt = self.__get_number_value_from_data_dict(check_black_info, 'contacts_class1_cnt')
        derivatives[self.mobile_service_prefix + 'UserInfoCheck_ContactsClass1Cnt'] = contacts_class1_cnt
        contacts_router_cnt = self.__get_number_value_from_data_dict(check_black_info, 'contacts_router_cnt')
        derivatives[self.mobile_service_prefix + 'UserInfoCheck_ContactsRouterCnt'] = contacts_router_cnt
        contacts_router_ratio = self.__get_number_value_from_data_dict(check_black_info, 'contacts_router_ratio')
        derivatives[self.mobile_service_prefix + 'UserInfoCheck_ContactsRouterRatio'] = contacts_router_ratio

        idcard_with_other_phones_cnt = len(check_search_info['idcard_with_other_phones'])
        derivatives[self.mobile_service_prefix + 'UserInfoCheck_IDPhoneCombinationCnt'] = idcard_with_other_phones_cnt
        return derivatives

    def __extract_cell_behavior_derivatives(self, cell_behavior):
        derivatives = {}
        cell_behavior = cell_behavior[0]
        cnt = len(cell_behavior['behavior'])
        net_flow_sum = sum([c['net_flow'] for c in cell_behavior['behavior']]) / 1000  # unit: M
        derivatives[self.mobile_service_prefix + 'CellBehavior_AvgNetFlow'] = net_flow_sum / cnt if cnt > 0 else 0
        total_amount_sum = sum([c['total_amount'] for c in cell_behavior['behavior']]) / 100  # unit: yuan
        derivatives[
            self.mobile_service_prefix + 'CellBehavior_AvgTotalAmount'] = total_amount_sum / cnt if cnt > 0 else 0
        sms_cnt_sum = sum([c['sms_cnt'] for c in cell_behavior['behavior']])
        derivatives[self.mobile_service_prefix + 'CellBehavior_AvgSmsCnt'] = sms_cnt_sum / cnt if cnt > 0 else 0
        return derivatives

    def __extract_behavior_check_derivatives(self, behavior_check):
        derivatives = {}
        phone_silent = [c for c in behavior_check if c['check_point'] == 'phone_silent']
        if phone_silent:
            phone_silent = phone_silent[0]
            derivatives[self.mobile_service_prefix + 'BehaviorCheck_PhoneSilentScore'] = int(phone_silent['score'])
            phone_silent_result = phone_silent['result']
            phone_silent_cnt = 0
            phone_silent_rate = 0.0
            try:
                x = phone_silent_result.split("天内有")
                y = x[1].split("天无通话记录")
                phone_silent_cnt = int(y[0])
                phone_silent_rate = phone_silent_cnt / int(x[0])
            except:
                pass
            derivatives[self.mobile_service_prefix + 'BehaviorCheck_PhoneSilentCnt'] = phone_silent_cnt
            derivatives[self.mobile_service_prefix + 'BehaviorCheck_PhoneSilentRate'] = phone_silent_rate

            phone_silent_evidence = phone_silent['evidence']
            phone_silent_max_continues_silent_days = 0
            try:
                phone_silent_max_continues_silent_days = \
                    max([int(r.split(',')[1].split('天')[0]) for r in phone_silent_evidence.split("：")[1].split('；')])
            except:
                pass
            derivatives[self.mobile_service_prefix + 'BehaviorCheck_PhoneSilentMostCnt'] = \
                phone_silent_max_continues_silent_days

            phone_used_time = [c for c in behavior_check if c['check_point'] == 'phone_used_time']
            if phone_used_time:
                phone_used_time = phone_used_time[0]
                phone_used_time_result = phone_used_time['result']
                phone_used_time_evidence = phone_used_time['evidence']
                time_numbers = self.__find_all_numbers(phone_used_time_result)
                if time_numbers and self.mobile_online_map.get(min(time_numbers)):
                    derivatives['X_Mobile_DurationOfOnline'] = self.mobile_online_map[min(time_numbers)]
                estimated_month = self.estimated_month_re.findall(phone_used_time_evidence)
                if estimated_month:
                    derivatives[self.mobile_service_prefix + 'BehaviorCheck_EstimatedOnlineMonth'] = int(
                        estimated_month[0])
        return derivatives

    def __extract_raw_data_derivatives(self, raw_data):
        derivatives = {}
        open_time = raw_data.get('open_time')
        if open_time:
            derivatives[self.mobile_service_prefix + 'RegisterDayCnt'] = \
                (datetime.now() - datetime.strptime(open_time, "%Y-%m-%d")).days

        derivatives['X_UserMobileProvince'] = raw_data['province']
        derivatives['X_UserMobileCity'] = raw_data['city']
        carrier = raw_data['carrier']
        if carrier in self.service_provider_map:
            derivatives['X_UserMobileServiceProvider'] = self.service_provider_map[carrier]
        # derivatives['X_Mobile_ThreeInfo'] = '0' if raw_data['reliability'] == 1 else '1'
        derivatives[self.mobile_service_prefix + 'basic_state'] = raw_data['state']
        derivatives[self.mobile_service_prefix + 'basic_level'] = raw_data['level']
        return derivatives

    @staticmethod
    def __get_number_value_from_data_dict(data, key, default=0):
        return int(data[key]) if data.get(key) else default

    @staticmethod
    def __find_all_numbers(string):
        numbers = re.findall(r'\d+', string)
        return [int(n) for n in numbers]
