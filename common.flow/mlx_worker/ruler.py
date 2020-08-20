import logging
import re
from box import Box
from random import random


class RuleResult:
    TRUE = 1
    FALSE = 0
    DEFAULT = 0
    SKIPPED = -1


class RuleType:
    BLACK_DIRECT = "A"    # 本人黑名单类
    DEBT = "B"            # 本人共债类
    BLACK_INDIRECT = "C"  # 社交涉黑类
    VALIDATION = "D"      # 核身类
    DELAY = "E"           # 其它平台分/买单侠逾期结果
    FRAUD = "F"           # 欺诈规则类
    CHECK_FINAL = "G"     # 终审规则类
    UNSET = "U"           # 未分类


class Ruler:
    def __init__(self, rule_collection):
        super().__init__()
        self.rc = RuleCompiler(rule_collection)

    def get_rule_vars(self):
        return self.rc.get_rule_vars()

    def run(self, data):
        return self.rc.run(data)


class RuleCompiler:
    RULE_VAR_RE = re.compile("{.+?}")

    def __init__(self, rule_collection):
        """
        rule collection example:
            {
              "id": "precheck_black",
              "version": "2017.09.08",
              "rules": [
                {
                  "code": "P001",
                  "rule": ["{X_QH_IsBlack} == True"],  # string or string list supported
                  "priority": 10,  # smaller, with higher priority
                  "proba": 1,  # probability for this rule to run, 0 to disable this rule
                  "r_true": 1,  # result value if rule hit
                  "r_false": 0,   # result value if rule not hit
                  "r_default": 0,  # result value if something unexpected happened
                  "r_skipped": -1  # result value if skipped
                },
                {
                  "code": "P002",
                  "rule": "{X_PY_IsBlack} == True",
                  "priority": 20
                },
                {
                  "code": "P003",
                  "rule": "{} == True",
                  "priority": 30
                },
                {
                  "code": "P004",
                  "rule": "{X_IsBlack.IdNo} == True",
                  "priority": 40
                }
              ],
              "combinator": {
                "any": [{"all": ["P001", "P002"]}, {"all": ["P003", "P004"]}]
              }
            }
        """
        self.id = rule_collection['id']
        self.version = rule_collection['version']
        self.rules = rule_collection['rules']
        self.combinator = rule_collection['combinator']

    def get_rule_vars(self):
        var_set = set()
        for r in self.rules:
            rule = r['rule']
            if isinstance(rule, list):
                rule = ' '.join(rule)
            vs = self.RULE_VAR_RE.findall(rule)
            for v in vs:
                var_set.add(v[1:-1])  # remove heading '{' and tailing '}'
        return list(var_set)

    def run(self, var_dict):
        """
        :return:
            {
                'rule_collection_id': 'precheck_id',
                'rule_collection_version': '2017.09.08',
                'rule_collection_result': 1,
                'first_hit': 'P001',
                'rule_results': [
                    ('P001', 1, 1, 'A'),  # code, result, result_actual, type
                    ('P002', 0, 0, 'B'),
                    ('P003', -1, 1, 'C'),
                    ('P004', -1, 0, 'D')
                ]
            }
        """
        rule_results = []
        first_hit = None
        for r in sorted(self.rules, key=lambda x: x['priority']):
            hit = False
            result = r.get('r_default', RuleResult.DEFAULT)
            result_actual = result  # actual rule result regardless of skipping or other situations
            try:
                rule = r['rule']
                if isinstance(rule, list):
                    rule = ' '.join(rule)
                hit = eval(rule.format(**self._preprocess_var_dict(var_dict)))
                result = r.get('r_true', RuleResult.TRUE) if hit else r.get('r_false', RuleResult.FALSE)
                result_actual = result
            except:
                logging.exception("eval rule result error! code: {}, rule: {}".format(r['code'], r['rule']))
            if 'proba' in r and r['proba'] <= random():  # rule skipped
                result = r.get('r_skipped', RuleResult.SKIPPED)
                hit = False
            rule_results.append((r['code'], result, result_actual, r.get('type', RuleType.UNSET)))
            if hit and not first_hit:  # not hit yet, i.e. this is the first hit rule
                first_hit = r['code']
        rule_collection_result = self._combine_rule_results(self.combinator, rule_results)
        return {
            'rule_collection_id': self.id,
            'rule_collection_version': self.version,
            'rule_collection_result': rule_collection_result,
            'first_hit': first_hit,
            'rule_results': rule_results
        }

    def _preprocess_var_dict(self, var_dict):
        var_dict = Box(var_dict, box_it_up=True)
        rule_vars = self.get_rule_vars()
        for rv in rule_vars:
            keys = rv.split('.')
            self._fill_var_dict(var_dict, keys)
        return var_dict

    def _fill_var_dict(self, var_dict, check_keys, default_value=None):
        key_to_check = check_keys[0]
        if len(check_keys) == 1:
            if key_to_check not in var_dict:
                var_dict[key_to_check] = default_value
            return
        if key_to_check not in var_dict:
            var_dict[key_to_check] = Box()
        self._fill_var_dict(var_dict[key_to_check], check_keys[1:])

    def _combine_rule_results(self, combinator, results):
        """
        recursively calculate combinator
        :param results:
            [
                ('P001', 1, 'A'),
                ('P002', 0, 'B'),
                ('P003', 0, 'C'),
                ('P004', 1, 'D')
            ]
        """
        k = list(combinator)[0]
        v = combinator[k]
        if len(v) == 0:
            v_list = []
        elif isinstance(v[0], str):
            # r[0]: code, r[1]: result
            v_list = [r[1] for r in results if r[0] in v and r[1] in (RuleResult.TRUE, RuleResult.FALSE)]
        else:
            v_list = [self._combine_rule_results(x, results) for x in v]
        if k == "all":
            return int(all(v_list))
        if k == "any":
            return int(any(v_list))
        if k == "sum":
            return sum(v_list)
        return None
