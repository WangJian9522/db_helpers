# -*- coding: utf-8 -*-
# @Time    : 2019/6/28 18:18
# @Author  : WJ

import pymongo
from sky_spiders.log_helper.logger import storage

# 数据库基本信息
db_configs = {
    'type': 'mongo',
    'host': '127.0.0.1',
    'port': '27017',
    "user": "",
    "password": "",
    'db_name': 'myDB'
}


class Mongo():
    def __init__(self):
        self.client = pymongo.MongoClient(f'mongodb://{db_configs["host"]}:{db_configs["port"]}')
        self.username = db_configs["user"]
        self.password = db_configs["passwd"]
        if self.username and self.password:
            self.db = self.client[db_configs["host"]].authenticate(db_configs["db_name"], self.password)
        self.db = self.client[db_configs["host"]]

    def find_data(self, col="InfoQ"):
        # 获取状态为0的数据
        data = self.db[col].find({"status": 0}, {"_id": 0})
        gen = (item for item in data)
        return gen
    # 更改状态
    def change_status(self, uuid, item, col="InfoQ", status_code=0):
        # status_code 0:初始,1:开始下载，2下载完了
        item["status"] = status_code
        self.db[col].update_one({'uuid': uuid}, {'$set': item})
    # 保存
    def save_data(self, items, col="InfoQ"):
        if isinstance(items, list):
            for item in items:
                try:
                    self.db[col].update_one({
                        'uuid': item.get("uuid")},
                        {'$set': item},
                        upsert=True)
                except Exception as e:
                    storage.error(f"数据插入出错:{e.args},此时的item是:{item}")
        else:
            try:
                self.db[col].update_one({
                    'uuid': items.get("uuid")},
                    {'$set': items},
                    upsert=True)
            except Exception as e:
                storage.error(f"数据插入出错:{e.args},此时的item是:{items}")


if __name__ == '__main__':
    m = Mongo()
    m.find_data()
