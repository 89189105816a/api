

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('92.51.46.138'))
channel = connection.channel()

channel.queue_declare(queue='python-queue', durable=True)

message = b'{"mpId_to_execute":[0, 0], "scripts": ["sales_wb", "orders_wb"], "interval": 1 }'
channel.basic_publish(exchange='',
                      routing_key='python-queue',
                      body=message,
                      properties=pika.BasicProperties(
                          delivery_mode=pika.DeliveryMode.Persistent
                      ))
print('[done]')
connection.close()
