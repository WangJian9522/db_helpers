# -*- coding: utf-8 -*-
# @Time    : 2019/7/4 13:37
# @Author  : WJ
from sky_spiders.settings.db_conf import RABBITMQ_CONFIG
import pika
import sys


# rabbitmq连接实例
class Mq_product:
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_CONFIG['username'], RABBITMQ_CONFIG['password'])
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_CONFIG['host'], credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='logs', exchange_type='fanout')

    def send_msg(self):
        message = ' '.join(sys.argv[1:]) or "info: Hello World!"
        self.channel.basic_publish(exchange='logs', routing_key='', body=message)
        print(" [x] Sent %r" % message)
        self.connection.close()
if __name__ == '__main__':
    mq = Mq_product()
    mq.send_msg()