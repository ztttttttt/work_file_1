class Flow(object):
    def __init__(self, flow_data):
        self.flows = flow_data['flows']
        self.dependencies = flow_data['dependencies']
        self.vars = {}
        self.finished_flows = set()

    def run(self):
        for flow in self.flows:
            if self.__is_runnable(flow['name']):
                self.__run_flow(flow)
        if not self.__is_finished():
            self.run()

    def draw(self):
        pass

    def __run_flow(self, flow):
        # construct parameters and run the flow
        real_func_params = []
        if 'params' in flow:
            params = flow['params']
            for param in params:
                param_type = param[0]
                param_value = None
                if param_type == "var":
                    param_value = self.vars[param[1]]
                elif param_type == "val":
                    param_value = param[1]
                else:
                    raise ValueError("Unknown parameter for flow: {0}".format(flow))
                real_func_params.append(param_value)
        real_returns = flow['func'](*tuple(real_func_params))

        # store return values as middle variables for the whole flow
        if 'return' in flow:
            expect_returns = flow['return']
            assert len(expect_returns) <= len(real_returns)
            if len(expect_returns) == 1:
                self.vars[expect_returns[0]] = real_returns
            else:
                for idx, val in enumerate(expect_returns):
                    self.vars[val] = real_returns[idx]

        # add flow to finished list
        self.finished_flows.add(flow['name'])

    def __is_runnable(self, flow_name):
        """
        judge if the flow runnable at the moment
        """
        if flow_name in self.finished_flows:    # skip finished flows
            return False

        for dep in self.dependencies:
            if flow_name in dep['downstream']:    # judge according to dependencies
                return self.finished_flows.issuperset(set(dep['upstream']))
        return True    # if not in any downstream, i.e. no dependencies, return True

    def __is_finished(self):
        return len(self.finished_flows) == len(self.flows)
