# -*- coding: utf-8 -*-
# @Time    : 2019/6/28 18:18
# @Author  : WJ

from sky_spiders.settings.db_conf import REDIS_CONFIG
from redis import Redis
class RedisHelper(object):
    def __init__(self):
        self.redis_client = Redis(REDIS_CONFIG['host'], REDIS_CONFIG['PASSWORD'])
        pass
    def save(self, dic_val):
        for key, value in dic_val.items():
            self.redis_client.set(key,value)
            self.redis_client.hset('name',key,value)
            print(key,value)

# if __name__ == '__main__':
#     red = RedisHelper()
#     red.save({'a':1, 'b':2})