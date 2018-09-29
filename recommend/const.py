# -*- coding: utf-8 -*-
"""常量定义"""

video_index = 'resources'
video_type = 'doc'

hot_video_key1 = 'hot_video_zset1'
hot_video_key2 = 'hot_video_zset2'


class ReturnCode(object):
    """返回码"""
    success = 0


class Operation(object):
    """操作类型"""
    watch = 1
    collect = 2
    share = 3
    star = 4
    dislike = 5
