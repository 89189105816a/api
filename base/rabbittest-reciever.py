
import pika, sys, os, time


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('92.51.46.138'))
    channel = connection.channel()
    channel.queue_declare(queue='python-queue', durable=True)

    def callback(ch, method, properties, body):
        time.sleep(10)
        print(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='python-queue',
                          auto_ack=False,
                          on_message_callback=callback)

    channel.basic_qos(prefetch_count=1)

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            print('zZz')
