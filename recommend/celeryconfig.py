# -*- coding: utf8 -*-
"""celery配置"""
from recommend.configure import AMQP_URL

broker_url = AMQP_URL

imports = (
    'recommend.tasks',
)

task_serializer = 'pickle'
accept_content = ['pickle', 'json']
