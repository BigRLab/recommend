# -*- coding: utf-8 -*-
"""
youtube 视频第二版推荐算法

召回环节通过比较标签相似度以及热门视频
排序环境通过视频播放量进行排序
和第一版没有本质区别, 只是需要根据视频id反查publish_id
"""
import re
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
    hot_video_key_v2,
    Operation,
)
from recommend.configure import PUBLISH_QUERY_URL
from recommend.algorithm.video import (
    stop_words_set,
    get_video,
)


emoji_pattern = re.compile('[\U0001F300-\U0001F64F\U0001F680-\U0001F6FF\u2600-\u2B55]+')
video_operation_score = {
    Operation.watch: 0.1,
    Operation.collect: 0.2,
    Operation.share: 0.3,
    Operation.star: 0.2,
    Operation.dislike: -0.5,
}


class VideoAlgorithmV2(object):

    def __init__(self):
        self.hot_videos = {}
        self._load_hot_videos()
        self._session = requests.Session()

    def _query_publish_id(self, videos_ids):
        result_map = {}
        if videos_ids:
            return result_map

        video_len = len(videos_ids)
        offset, limit = 0, 100
        while offset < video_len:
            s = videos_ids[offset: offset+limit]
            body = {
                'resources': [{'res_type': 'video', 'res_id': x} for x in s]
            }
            res = self._session.post(PUBLISH_QUERY_URL, json=body).json()
            for item in res['data']:
                if not item['pub_ids']:
                    continue
                result_map[item['res_id']] = item['pub_ids'][0]

            offset += limit
        return result_map

    def _load_hot_videos(self):
        """加载热门视频"""
        if redis_client.exists(hot_video_key_v2):
            videos = redis_client.zrangebyscore(
                hot_video_key_v2, '-inf', '+inf', withscores=True)
            for key, value in videos:
                self.hot_videos[key.decode('utf8')] = value
        else:
            video_map = {}
            video_map.update(self._get_hot_videos(size=700))
            video_map.update(self._get_hot_videos(tag='india', size=200))
            video_map.update(self._get_hot_videos(tag='bollywood', size=500))
            video_map.update(self._get_hot_videos(tag='series', size=200))

            video_publish_map = self._query_publish_id(list(video_map.keys()))

            zset_args = []
            for key, value in video_map.items():
                if key not in video_publish_map:
                    continue
                redis_key = '{}|{}'.format(key, video_publish_map[key])
                zset_args.append(value)
                zset_args.append(redis_key)
                self.hot_videos[redis_key] = value
            redis_client.zadd(hot_video_key_v2, *zset_args)

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
                        {'term': {'genre': 'youtube'}},
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
        title = source['title']
        video_tag = source.get('tag', [])
        video_tag.append(title)
        sentence = ' '.join(video_tag)
        sentence = sentence.lower()
        sentence = emoji_pattern.sub(' ', sentence)
        sentence = sentence.replace(",", " ").replace("|", " ").replace("#", " "). \
            replace("@", " ").replace("~", " ").replace("'", " ").replace("\"", " "). \
            replace("\\", " ").replace("/", " ").replace("_", " ").replace("-", " "). \
            replace("[", " ").replace("]", " ").replace("+", " ").replace("*", " "). \
            replace("{", " ").replace("}", " ").replace(";", " ").replace(":", " "). \
            replace("`", " ").replace("=", " ").replace("【", " ").replace("】", " "). \
            replace("(", " ").replace(")", " ").replace(".", " ").replace("’", " "). \
            replace("?", " ")
        words = sentence.split(' ')

        tags = set()
        for word in words:
            if not word:
                continue

            if len(word) == 1 or len(word) > 30:
                continue

            if word in stop_words_set:
                continue

            tags.add(word)
        return tags

    @staticmethod
    def _query_videos_by_tag(tags, size=100):
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
            '_source': ['hot'],
            'min_score': 20.0
        }
        query_result = es_client.search(video_index, video_type, body=query)
        hits = query_result['hits']['hits']

        video_map = {}
        for item in hits:
            id_ = item['_id']
            hot = item['_source']['hot']
            if hot > 100000:
                video_map[id_] = hot
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

        if not tags:
            return

        video_map = self._query_videos_by_tag(tags, size)
        if video_id in video_map:
            video_map.pop(video_id)

        video_publish_map = self._query_publish_id(list(video_map.keys()))
        result_map = {}
        for key, value in video_map.items():
            if key not in video_publish_map:
                continue
            redis_key = '{}|{}'.format(key, video_publish_map[key])
            result_map[redis_key] = value
        return result_map

    def update_recommend_list(self, device, video, operation):
        """针对用户操作视频的行为更新推荐列表

        Args:
            device (str): 设备id
            video (str): 视频id
            operation (int): 操作类型
        """
        device_key = 'device|{}|recommend|v2'.format(device)
        recommend_list = redis_client.zrangebyscore(
            device_key, '-inf', '+inf', withscores=True, start=0, num=1000)
        if not recommend_list:
            return

        video_map = self.get_similar_videos(video, 20)
        if not video_map:
            return

        recommend_map = {key.decode('utf8'): value for key, value in recommend_list}
        recommend_map[video] = (time.time() - 2147483647) / 200000000
        for key, value in video_map.items():
            if key in recommend_map:
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
                if recommended >= 500:    # 最近500条视频不重复
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
        device_key = 'device|{}|recommend|v2'.format(device)
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
        score = (time.time() - 2147483647) / 200000000
        for video in recommend_videos:
            zset_args.append(score)
            zset_args.append(video)
        redis_client.zadd(device_key, *zset_args)
        return recommend_videos


algorithm2 = VideoAlgorithmV2()
