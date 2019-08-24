# -*- coding: utf-8 -*-
# @Time    : 2019/7/4 13:37
# @Author  : WJ

from sky_spiders.settings.db_conf import RABBITMQ_CONFIG
import pika
import sys

# 哈哈
class consumer:
    """消费者"""
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_CONFIG['username'], RABBITMQ_CONFIG['password'])
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_CONFIG['host'], credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='logs', exchange_type='fanout')

    def use_msg(self):
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange='logs', queue=queue_name)

        print(' [*] Waiting for logs. To exit press CTRL+C')

        def callback(ch, method, properties, body):
            print(" [x] %r" % body)

        self.channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)

        self.channel.start_consuming()

if __name__ == '__main__':
    mq = consumer()
    mq.use_msg()