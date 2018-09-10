# -*- coding: utf-8 -*-
"""参数配置"""
import os
import consul
import ujson

CONSUL_HOST = '172.31.23.5'
CONSUL_PORT = 8500
CONSUL_PREFIX = 'service/recommend/production'


class ConfigHandler(object):
    """配置基类"""

    def __init__(self, host, port, prefix):
        self.prefix = prefix
        self.client = consul.Consul(host=host, port=port)

    def get(self, key):
        param_key = os.path.join(self.prefix, key)
        _, val = self.client.kv.get(param_key)
        value = val['Value']
        return value.decode('utf8')

config_handler = ConfigHandler(CONSUL_HOST, CONSUL_PORT, CONSUL_PREFIX)


MYSQL_URL = config_handler.get('MYSQL_URL')
REDIS_URL = config_handler.get('REDIS_URL')
AMQP_URL = config_handler.get('AMQP_URL')
ES_HOSTS = ujson.loads(config_handler.get('ES_HOSTS'))
