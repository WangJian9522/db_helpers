# -*- coding: utf-8 -*-
# @Time    : 2019/6/28 18:18
# @Author  : WJ

import asyncio
from sky_spiders.log_helper.logger import storage
from motor.motor_asyncio import AsyncIOMotorClient
from bson import SON  # 二进制json
import pprint
from sky_spiders.settings.db_conf import MONGODB_CONFIG as db_configs

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

class MotorBase():
    def __init__(self):

        if db_configs['user']:
            self.motor_uri = f"mongodb://{db_configs['user']}:{db_configs['passwd']}@{db_configs['host']}:{db_configs['port']}/{db_configs['db_name']}?authSource={db_configs['user']}"
        self.motor_uri = f"mongodb://{db_configs['host']}:{db_configs['port']}/{db_configs['db_name']}"
        self.client = AsyncIOMotorClient(self.motor_uri)
        self.db = self.client.myDB

    async def save_data(self, item):
        try:
            await self.db.infoq_details.update_one({
                'uuid': item.get("uuid")},
                {'$set': item},
                upsert=True)
        except Exception as e:
            storage.error(f"数据插入出错:{e.args}此时的item是:{item}")

    async def change_status(self, uuid, status_code=0):
        # status_code 0:初始,1:开始下载，2下载完了
        # storage.info(f"修改状态,此时的数据是:{item}")
        item = {}
        item["status"] = status_code
        await self.db.infoq_seed.update_one({'uuid': uuid}, {'$set': item}, upsert=True)

    async def reset_status(self):
        await self.db.infoq_seed.update_many({'status': 1}, {'$set': {"status": 0}})

    async def reset_all_status(self):
        await self.db.infoq_seed.update_many({}, {'$set': {"status": 0}})

    async def get_detail_datas(self):
        data = self.db.infoq_seed.find({'status': 1})

        async for item in data:
            print(item)
        return data

    async def find(self):
        data = self.db.InfoQ.find({'status': 0})
        async_gen = (item async for item in data)
        return async_gen

    async def use_count_command(self):
        response = await self.db.command(SON([("count", "infoq_seed")]))
        print(f'response:{pprint.pformat(response)}')


if __name__ == '__main__':
    m = MotorBase()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(m.find())