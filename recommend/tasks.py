# -*- coding: utf-8 -*-
"""celery 任务"""
from recommend import celery_app
from recommend.algorithm.video.v1 import algorithm1
from recommend.algorithm.video.v2 import algorithm2


@celery_app.task
def update_video_recommendation(device, video_id, operation):
    """根据用户行为更新推荐内容

    Args:
        device (str): 设备id
        video_id (str): 视频id
        operation (int): 操作类型
    """
    if device[0] in ('0', '1', '2', '3', '4', '5', '6', '7'):
        algorithm1.update_recommend_list(device, video_id, operation)
    else:
        algorithm2.update_recommend_list(device, video_id, operation)
