# -*- coding: utf-8 -*-
# @Time    : 2019/6/28 18:17
# @Author  : WJ
from sky_spiders.settings.db_conf import MYSQL_CONFIG
import MySQLdb


class MysqlHelper(object):
    def __init__(self):
        self.mysql_client = MySQLdb.connect(host=MYSQL_CONFIG["host"],
                                            port=MYSQL_CONFIG["port"],
                                            user=MYSQL_CONFIG["user"],
                                            passwd=MYSQL_CONFIG["passwd"],
                                            db=MYSQL_CONFIG["db"],
                                            charset=MYSQL_CONFIG["charset"])
        self.cursor = self.mysql_client.cursor()

    def insert_db(self, table_name, column_name, data):
        """

        :param table_name: 表名
        :param column_name: 列名，是一个字符串
        :param data: 插入的数据，列表或元组
        :return:
        """
        sql = f'''insert into {table_name}({column_name}) values ({('%s,' * len(data))[:-1]})'''
        self.cursor.execute(sql, data)
        self.mysql_client.commit()

    def update_db(self, table, data):
        for key, value in data.items():
            sql = f"""update {table} set  {key} = '{value}'"""
            self.cursor.execute(sql)
            self.mysql_client.commit()

    def del_db(self, table, data):
        for key, value in data.items():
            sql = f'delete from {table} where {key} = {value}'
            self.cursor.execute(sql)
            self.mysql_client.commit()


    def count_db(self, table, column, limit=None):
        global sql
        if limit and isinstance(limit, dict):
            for key, value in limit.items():
                sql = f"""select count({column}) from {table} where {key}='{value}'"""
        else:
            sql = f'select count({column}) from {table}'
        return self.cursor.execute(sql)
    def search_db(self, table, column):
        sql = f"""select {column} from {table} order by age desc """
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        for row in results:
            print(row)
            # ID = row[0]
            # Name = row[1]
            # Grade = row[2]
            # # 打印结果
            # print("ID: %s, Name: %s, Grade: %d" % (ID, Name, Grade))
    def close(self):
        self.cursor.close()
        self.mysql_client.close()

s = MysqlHelper()
# print(s.count_db(table='temp', column='name', limit={'name':'小明'}))
# s.update_db('temp', data={'name':'小明'})
s.search_db(table='temp',column='*')
s.close()