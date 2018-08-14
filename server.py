# -*- coding: utf8 -*-
"""用户接口"""
from flask import jsonify
from webargs import fields

from recommend import (
    flask_app,
    tasks,
)
from recommend.const import ReturnCode
from recommend.tools.args import parser
from recommend.algorithm.video.v1 import algorithm
from recommend.tools.trace import logger


@flask_app.route('/recommend/video/guess-like', methods=['GET'])
@parser.use_args({
    'id': fields.Str(required=True, location='query'),
})
def video_guess_like(args):
    video_id = args['id']
    videos = algorithm.get_similar_videos(video_id)
    return jsonify({
        "code": ReturnCode.success,
        "result": "ok",
        "data": videos,
    })


@flask_app.route('/recommend/device/video/behavior', methods=['POST'])
@parser.use_args({
    'device': fields.Str(required=True, location='json'),
    'video_id': fields.Str(required=True, location='json'),
    'operation': fields.Int(required=True, location='json'),
})
def device_video_behavior(args):
    device = args['device']
    video_id = args['video_id']
    operation = args['operation']
    if video_id:
        tasks.update_video_recommendation.delay(device, video_id, operation)
    return jsonify({
        "code": ReturnCode.success,
        "result": "ok",
    })


@flask_app.route('/recommend/device/video/recommend', methods=['GET'])
@parser.use_args({
    'device': fields.Str(required=True, location='query'),
    'size': fields.Int(location='query'),
})
def device_video_recommend(args):
    device = args['device']
    size = args.get('size', 10)
    logger.info('device: {} size:{}'.format(device, size))
    videos = algorithm.get_recommend_videos(device, size)
    return jsonify({
        "code": ReturnCode.success,
        "result": "ok",
        "data": videos,
    })

if __name__ == '__main__':
    flask_app.run(host='0.0.0.0', port=30001)
