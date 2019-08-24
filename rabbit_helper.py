# -*- coding: utf-8 -*-
# @Time    : 2019/6/28 18:19
# @Author  : WJ
from sky_spiders.settings.db_conf import RABBITMQ_CONFIG
import pika
import sys

class MqHelper:
    def __init__(self):
        self.credentials = pika.PlainCredentials(RABBITMQ_CONFIG['username'], RABBITMQ_CONFIG['password'])
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_CONFIG['host'], credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='sky_spider', exchange_type='topic')

    def send_msg(self, routing_key, msg):
        """发送消息"""
        self.channel.basic_publish(exchange='sky_spider', routing_key=routing_key, body=msg)

    def receive_msg(self, binding_key, callback):
        """接收消息"""
        result = self.channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue
        # binding_keys = sys.argv[1:]
        # if not binding_keys:
        #     sys.stderr.write("使用: %s [binding_key]...\n" % sys.argv[0])
        #     sys.exit(1)

        # for binding_key in binding_keys:
        self.channel.queue_bind(exchange='sky_spider', queue=queue_name, routing_key=binding_key)
        print('Waiting receive.....')

        def mq_callback(ch, method, properties, body):
            return callback({'routing_key':method.routing_key, "body":body, 'ch':ch, 'properties':properties})

        self.channel.basic_consume(queue=queue_name, on_message_callback=mq_callback,auto_ack=True)
        self.channel.start_consuming()

    def close(self):
        """关闭连接"""
        self.connection.close()
