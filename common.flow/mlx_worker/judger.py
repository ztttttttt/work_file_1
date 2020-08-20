import logging


class Judger:
    def __init__(self, judger):
        self.jc = JudgeCompiler(judger)

    def get_rule_collection_vars(self):
        """
        get all the rule collection variable names used in the judge
        """
        return self.jc.rule_collections

    def get_tag_vars(self):
        """
        get all the tag variable names in the judge
        """
        return self.jc.tags

    def run(self, var_dict):
        return self.jc.run(var_dict)


class JudgeCompiler:
    def __init__(self, judger):
        """
        judger example:
            {
              "id": "precheck_id_judger",
              "version": "2017.09.08",
              "tags": ["Invincible"],
              "rule_collections": ["precheck_id"],
              "models": ["ml_risk_model_result"],
              "judges": [
                {
                  "judge": ["{Invincible} == True"],  # string or string list supported
                  "priority": 10,
                  "result": "pass",
                  "extension": "invincible_tag"
                },
                {
                  "judge": "{precheck_id} == 1",
                  "priority": 20,
                  "result": "reject",
                  "extension": "rc"
                },
                {
                  "judge": "True",
                  "priority": 30,
                  "result": "pass",
                  "extension": "rc"
                }
              ]
            }
        """
        self.id = judger['id']
        self.version = judger['version']
        self.tags = judger.get('tags', [])
        self.rule_collections = judger.get('rule_collections', [])
        self.models = judger.get('models', [])
        self.judges = judger['judges']

    def run(self, var_dict):
        var_dict = self._fill_var_dict(var_dict, [*self.tags, *self.rule_collections, *self.models])
        for j in sorted(self.judges, key=lambda x: x['priority']):
            try:
                judge = j['judge']
                if isinstance(judge, list):
                    judge = ' '.join(judge)
                if eval(judge.format(**var_dict)):
                    return {
                        'judge_id': self.id,
                        'judge_version': self.version,
                        'result': j['result'],
                        'extension': j['extension']
                    }
            except:
                logging.exception("eval judge result error! judge: {}".format(j['judge']))

    def _fill_var_dict(self, var_dict, check_keys, default_value=None):
        for k in check_keys:
            if k not in var_dict:
                var_dict[k] = default_value
        return var_dict
