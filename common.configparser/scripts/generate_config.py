#!/usr/bin/env python
from mlx_configparser import template_config_parser as tcp
import os

class GenerateConfig(object):
    def __init__(self):
        self.template_config_file = "./config.tpl.yaml"
        self.config_data_file = "./config.json"
        self.last_config_file = "./config.last.yaml"
        self.config_file = "./config.yaml"
        self.generate_config()

    def generate_config(self):
        if os.path.isfile(self.config_file):
            os.rename(self.config_file, self.last_config_file)
        parser = tcp.TemplateConfigParser(self.template_config_file, self.config_data_file, self.config_file)
        parser.parse_and_save()


class FlowController(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_flow_controller/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_flow_controller/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_flow_controller/config.yaml"
        self.generate_config()


class Worker(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_worker/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_worker/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_worker/config.yaml"
        self.generate_config()


class Graph(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_graph/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_graph/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_graph/config.yaml"
        self.generate_config()


class Category(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_category/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_category/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_category/config.yaml"
        self.generate_config()


class Ruler(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_ruler/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_ruler/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_ruler/config.yaml"
        self.generate_config()


class Model(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_ml_model/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_ml_model/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_ml_model/config.yaml"
        self.generate_config()


class CreditAmount(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_credit_amount/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_credit_amount/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_credit_amount/config.yaml"
        self.generate_config()


class Judger(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_judger/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_judger/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_judger/config.yaml"
        self.generate_config()


class Rest(GenerateConfig):
    def __init__(self):
        self.template_config_file = "/home/lian/ml-x/jdxserver/jdx_ml_rest/config.tpl.yaml"
        self.config_data_file = "/home/lian/ml-x-config/configdata/qa/config.json"
        self.last_config_file = "/home/lian/ml-x/jdxserver/jdx_ml_rest/config.last.yaml"
        self.config_file = "/home/lian/ml-x/jdxserver/jdx_ml_rest/config.yaml"
        self.generate_config()

def main():
    FlowController()
    Worker()
    Graph()
    Category()
    Ruler()
    Model()
    CreditAmount()
    Judger()
    Rest()


if __name__ == '__main__':
    main()
