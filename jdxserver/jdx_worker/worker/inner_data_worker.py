import json


class InnerDataWorker(object):
    def __init__(self):
        self.prefix = 'X_User_addressBook_'
        self.ylzh_prefix = 'X_YLZC_'
        self.jztl_prefix = 'X_JZTL_'

    def run(self, derivate_data):
        address_book_d = json.loads(derivate_data.get('X_User_addressBook', '{}'))
        ylzc_d = derivate_data.get('X_YLZC', {})
        jztl_d = derivate_data.get('X_JZTL', {})
        # user_id = derivate_data.get('X_UserId')
        source = derivate_data.get('X_Origin_source')
        entrance = derivate_data.get('X_Origin_entrance')
        # group_tag = raw_data.get('X_DNA_CtrlGroup', 'A')
        # card_group_tag = raw_data.get('X_DNA_Card_CtrlGroup', 'A')
        # is_firstReLoan = raw_data.get('X_FirstReLoanMark')
        derivatives = {}

        derivatives.update(self.__resolve_source_data(source, entrance))

        if address_book_d:
            if type(address_book_d) is list:
                address_book_d = address_book_d[0]
            derivatives.update(self.__resolve_contact_data(address_book_d.get('contents', [])))

        if ylzc_d:
            if type(ylzc_d) is list:
                ylzc_d = ylzc_d[0]
            derivatives.update(self._resolve_ylzc_data(ylzc_d))

        if jztl_d:
            derivatives.update(self._resolve_jztl_data(jztl_d))

        return derivatives

    def __resolve_contact_data(self, contacts):
        derivatives = {}
        derivatives['X_User_addressBook_Cnt'] = len(contacts)
        return derivatives

    def _resolve_ylzc_data(self, result):
        derivatives = {}
        account_no = result.pop('accountNo')
        yearmonth = result.pop('yearmonth')
        derivatives[self.ylzh_prefix + 'yearmonth'] = yearmonth
        derivatives[self.ylzh_prefix + 'accountNo'] = account_no
        derivatives = {**derivatives, **{self.ylzh_prefix + key.upper(): value for key, value in result.items()}}
        return derivatives

    def _resolve_jztl_data(self, result):
        derivatives = {self.jztl_prefix + key: value for key, value in result.items()}
        return derivatives

    def __resolve_source_data(self, source, entrance):
        derivatives = {}
        # if entrance in ('1', '5', '21', '25'):
        #     source = source + entrance
        # if entrance and entrance[-1] in ('8',):
        #     source = source + entrance[-1]
        derivatives['X_JDX_Source'] = source
        derivatives['X_origin_entrance_mix'] = entrance
        # # 控制组
        # if group_tag == 'B':
        #     derivatives['X_JDX_Source'] = 'TagB'
        # if (card_group_tag == "B") and (is_firstReLoan in ['true',True]):
        #     derivatives['X_JDX_Source'] = 'TagB'
        return derivatives
