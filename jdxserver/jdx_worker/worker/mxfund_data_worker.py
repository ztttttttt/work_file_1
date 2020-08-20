import json
from mlx_utility import resolve_data


class MxFundDataWorker(object):
    def __init__(self):
        self.prefix = 'X_MX_Fund_'

    def _turn_to_json(self, json_str, times=3):
        result = json.loads(json_str)
        if isinstance(result, (str)) and times > 0:
            return self._turn_to_json(result, times - 1)
        else:
            return result

    def run(self, derivate_data):
        report = derivate_data.get('X_MX_PensionReport')
        raw = derivate_data.get('X_MX_Pension')
        derivatives = {}
        if raw:
            raw = self._turn_to_json(raw)
            raw = self._turn_to_json(raw.get('jsonData'))
            derivatives.update(resolve_data.resolve_data(raw, self.prefix + 'raw_data_'))
        if report:
            report = self._turn_to_json(report)
            report = self._turn_to_json(report.get('jsonData'))
            derivatives.update(resolve_data.resolve_data(report, self.prefix + 'raw_report_'))

        return derivatives
