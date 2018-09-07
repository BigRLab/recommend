# -*- coding: utf-8 -*-
"""
youtube 视频第一版推荐算法

召回环节通过比较标签相似度以及热门视频
排序环境通过视频播放量进行排序
"""
import time
import random
from math import log10
import requests
from recommend.models import (
    es_client,
    redis_client,
    cache_region,
)
from recommend.const import (
    video_index,
    video_type,
    hot_video_key,
    Operation,
)
from recommend.algorithm.video import (
    stop_words_set,
    get_video,
    get_videos,
)


video_operation_score = {
    Operation.watch: 0.1,
    Operation.collect: 0.2,
    Operation.share: 0.3,
    Operation.star: 0.2,
}


class VideoAlgorithmV1(object):

    def __init__(self):
        self.hot_videos = {}
        self._load_hot_videos()
        self._session = requests.Session()

    def _load_hot_videos(self):
        """加载热门视频"""
        if redis_client.exists(hot_video_key):
            videos = redis_client.zrangebyscore(
                hot_video_key, '-inf', '+inf', withscores=True)
            for key, value in videos:
                self.hot_videos[key.decode('utf8')] = value
        else:
            self.hot_videos.update(self._get_hot_videos(size=700))
            self.hot_videos.update(self._get_hot_videos(tag='india', size=200))
            self.hot_videos.update(self._get_hot_videos(tag='bollywood', size=500))
            self.hot_videos.update(self._get_hot_videos(tag='series', size=200))

            zset_args = []
            for key, value in self.hot_videos.items():
                zset_args.append(value)
                zset_args.append(key)
            redis_client.zadd(hot_video_key, *zset_args)

    @staticmethod
    def _get_hot_videos(tag=None, size=100):
        """找到制定标签的热门视频

        Args:
            tag (str): 标签
            size (int): 个数
        """
        query = {
            'size': size,
            'query': {
                'bool': {
                    'must': [
                        {'term': {'type': 'mv'}},
                    ]
                }
            },
            '_source': ['hot'],
            'sort': [{"hot": {"order": "desc"}}]
        }
        if tag:
            query['query']['bool']['must'].append({'term': {'tag': tag}})

        query_result = es_client.search(video_index, video_type, body=query)
        hits = query_result['hits']['hits']

        video_map = {}
        for item in hits:
            id_ = item['_id']
            view_count = item['_source']['hot']
            if view_count < 20000000:  # 热门视频的标准是观看数必须超过两千万
                continue
            video_map[id_] = log10(view_count)
        return video_map

    @staticmethod
    def _get_video_tag(video_id):
        """从es中找到视频, 并计算视频的标签向量

        Args:
            video_id (str): 视频id
        """
        source = get_video(video_id)
        video_tag = source.get('tag', [])

        tags = set()
        for item in video_tag:
            words = item.split()
            for word in words:
                w = word.lower()
                if w in stop_words_set:
                    continue
                if len(w) < 3:
                    continue
                if w.isalpha():
                    tags.add(w)
        return tags

    def _query_videos_by_tag(self, tags, size=100):
        """根据标签在es中查询视频

        Args:
            tags (set): 标签集合
            size (int): 视频个数
        """
        if not tags:
            return

        query = {
            'size': size,
            'query': {
                'bool': {
                    'must': [
                        {'term': {'type': 'mv'}},
                        {'term': {'genre': 'youtube'}},
                        {'term': {'status': 1}},
                        {
                            'bool': {
                                'should': [{'term': {'tag': x}} for x in tags]
                            }
                        }
                    ]
                }
            },
            '_source': ['hot', 'poster'],
            'min_score': 20.0
        }
        query_result = es_client.search(video_index, video_type, body=query)
        hits = query_result['hits']['hits']

        video_map = {}
        for item in hits:
            id_ = item['_id']
            hot = item['_source']['hot']
            poster = item['_source']['poster']
            try:
                if hot > 100000:
                    if self._session.head(poster, timeout=1).status_code == 200:
                        video_map[id_] = hot
            except:
                pass
        return video_map

    @cache_region.cache_on_arguments(expiration_time=3600)
    def get_similar_videos(self, video_id, size=10):
        """根据标签获取相似的视频(如果没有,则返回热门视频)

        Args:
            video_id (str): 视频id
            size (int): 数量
        """
        try:
            tags = self._get_video_tag(video_id)
        except:
            # 从youtube爬到视频信息出异常
            tags = None

        video_map = None
        if tags:
            video_map = self._query_videos_by_tag(tags, size)
        if video_map:
            video_ids = list(video_map.keys())
        else:
            video_ids = random.sample(self.hot_videos.keys(), size)

        if video_id in video_ids:
            video_ids.remove(video_id)
        return get_videos(video_ids)

    def update_recommend_list(self, device, video, operation):
        """针对用户操作视频的行为更新推荐列表

        Args:
            device (str): 设备id
            video (str): 视频id
            operation (int): 操作类型
        """
        device_key = 'device|{}|recommend'.format(device)
        recommend_list = redis_client.zrangebyscore(
            device_key, '-inf', '+inf', withscores=True, start=0, num=1000)
        if not recommend_list:
            return

        try:
            tags = self._get_video_tag(video)
        except:
            tags = None
        # 视频没有标签 不推荐数据
        if not tags:
            return

        video_map = self._query_videos_by_tag(tags, 20)
        if not video_map:
            return

        recommend_map = {key.decode('utf8'): value for key, value in recommend_list}
        recommend_map[video] = int(time.time()) - 2147483647
        for key, value in video_map.items():
            if key in recommend_map:
                if recommend_map[key] > 0:
                    recommend_map[key] += video_operation_score[operation] * log10(value)
            else:
                recommend_map[key] = log10(value)

        recommend_list = sorted(recommend_map.items(), key=lambda kv: kv[1], reverse=True)
        recommending, recommended = 0, 0
        zset_args = []
        for key, value in recommend_list:
            if value > 0:
                if recommending >= 500:   # 一个用户最多有500个推荐视频
                    continue

                zset_args.append(value)
                zset_args.append(key)
                recommending += 1
            else:
                if recommended >= 100:    # 最近300条视频不重复
                    continue

                zset_args.append(value)
                zset_args.append(key)
                recommended += 1

        redis_client.delete(device_key)
        redis_client.zadd(device_key, *zset_args)

    def get_recommend_videos(self, device, size):
        """获取推荐视频数据

        Args:
            device (str): 设备id
            size (int): 个数
        """
        device_key = 'device|{}|recommend'.format(device)
        recommend_list = redis_client.zrevrangebyscore(
            device_key, min=0, max='+inf', withscores=True, start=0, num=size)
        recommend_list = [x[0].decode('utf8') for x in recommend_list]

        # 推荐列表为空
        if not recommend_list:
            redis_client.delete(device_key)
            video_ids = random.sample(self.hot_videos.keys(), 200)
            recommend_videos = video_ids[:size]
            zset_args = []
            for video in video_ids[size:]:
                zset_args.append(1.0)
                zset_args.append(video)
            redis_client.zadd(device_key, *zset_args)
            redis_client.expire(device_key, 2592000)
        else:
            recommend_videos = recommend_list

        # 把推荐过的权值换成当前时间戳的负值,防止重复推荐
        zset_args = []
        score = int(time.time()) - 2147483647
        for video in recommend_videos:
            zset_args.append(score)
            zset_args.append(video)
        redis_client.zadd(device_key, *zset_args)
        return recommend_videos


algorithm = VideoAlgorithmV1()
