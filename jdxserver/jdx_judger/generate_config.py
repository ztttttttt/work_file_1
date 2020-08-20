#!/usr/bin/env python
from mlx_configparser import template_config_parser as tcp
import os

template_config_file = "./config.tpl.yaml"
config_data_file = "/home/ml/projects/ml-jdx-config/qa/config.json"
last_config_file = "./config.last.yaml"
config_file = "./config.yaml"

if os.path.isfile(config_file):
    os.rename(config_file, last_config_file)
parser = tcp.TemplateConfigParser(template_config_file, config_data_file, config_file)
parser.parse_and_save()
