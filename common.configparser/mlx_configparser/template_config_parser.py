import json
import re


class TemplateConfigParser(object):
    def __init__(self, template_file, config_data_file, save_file='./config.yaml', str_wrapper='"'):
        """
        Parse and save template config file to deploy-ready config file.
        :param template_file: template file is stored with code,
            containing config variables with format {{xxx.xxx}}
        :param config_data_file: config data file is stored separately and managed by Ops. It is in json format.
        :param save_file: parsed deploy-ready file name.
        :param str_wrapper: by default, {{xxx.xxx}} -> "yyyyy" since str_wrapper='"',
            if str_wrapper='', {{xxx.xxx}} -> yyyyy
        """
        super(TemplateConfigParser, self).__init__()
        self.template = template_file
        self.config_data = config_data_file
        self.save_file = save_file
        self.str_wrapper = str_wrapper
        self.tpl_re = re.compile("{{.+}}")

    def parse(self):
        """
        template example: config.template.yaml
        --------------------------------------------
            mysql_corvus:
                server: {{mysql_corvus.server}}
                port: {{mysql_corvus.port}}
                user: {{mysql_corvus.user}}
                password: {{mysql_corvus.password}}
                database: {{mysql_corvus.database}}

        config data example: config.json
        --------------------------------------------
            {
                "mysql_corvus": {
                    "server": "xx.xx.xx.xx",
                    "port": 1234,
                    "user": "aaa",
                    "password": "bbb",
                    "database": "ccc"
                }
            }

        parsed config example: config.yaml
        --------------------------------------------
            mysql_corvus:
                server: "xx.xx.xx.xx"
                port: 1234
                user: "aaa"
                password: "bbb"
                database: "ccc"
        """
        with open(self.config_data) as df:
            data = json.load(df)
        with open(self.template) as tf:
            tpl = tf.read()
        tpl_set = set(self.tpl_re.findall(tpl))
        for t in tpl_set:
            tpl = tpl.replace(t, self.__get_parsed_value(t, data))
        return tpl

    def parse_and_save(self):
        parsed = self.parse()
        with open(self.save_file, 'w') as f:
            f.write(parsed)

    def __get_parsed_value(self, tpl, data):
        keys = tpl[2:-2].split('.')  # remove template wrapper '{{}}', and split to keys
        value = data
        for k in keys:
            if k not in value:
                raise KeyError("template key not found: {}".format(tpl))
            value = value.get(k)
        wrapper = self.str_wrapper
        if isinstance(value, (int, float, list, dict)):
            wrapper = ''
        return "{0}{1}{0}".format(wrapper, value)
