
import pika, sys, os, time, json

from tasks.helpers.BaseClasses import DBHandler
from tasks._stocks_wb import stocks_wb
from tasks._sales_daily_temp import sales_daily_wb
from tasks._orders_daily_temp import orders_daily_wb
from dotenv import load_dotenv
from tasks.helpers.config_data import InitialData
from decorators.logger_deco import DecoCount

load_dotenv('.back_env')
rbmq_host = os.getenv('RABBITMQ_HOST')
rbmq_port = os.getenv('RABBITMQ_PORT')
rbmq_user = os.getenv('RABBITMQ_USER')
rbmq_pass = os.getenv('RABBITMQ_PASS')

# dear god here we go
dbobj = DBHandler()
all_clients_dataframe = dbobj.get_all_clients()

available_methods = {
    "sales_wb": sales_daily_wb,
    "orders_wb": orders_daily_wb
}


def main():
    credentials = pika.PlainCredentials(username=rbmq_user, password=rbmq_pass)
    connection_params = pika.ConnectionParameters(host=rbmq_host, port=rbmq_port, credentials=credentials)

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    # establishing queue just in case it does not exist
    channel.queue_declare(queue='python-queue', durable=True)

    def callback(ch, method, properties, body):
        json_string = body.decode('utf-8')
        body = json.loads(json_string)
        print(body["mpId_to_execute"])
        # { "mpId_to_execute":[1,2,3], "scripts": ["sales_wb", "orders_wb"], "interval": 1 }
        if body["mpId_to_execute"] == 'all':
            clients_list = all_clients_dataframe['mpid'].tolist()
            for mpid in clients_list:
                sales_daily_wb(mpid, body["interval"], 'wildberries-get')
                orders_daily_wb(mpid, body["interval"], 'wildberries-get')
        else:
            for id_ in body["mpId_to_execute"]:
                print(f"--- executing scripts for mpId {id_}")
                try:
                    for script_name in body["scripts"]:
                        available_methods[script_name](id_, body["interval"])
                except Exception as e:
                    print(e)

            time.sleep(3)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        DecoCount.log_export()

    channel.basic_consume(queue='python-queue',
                          auto_ack=False,
                          on_message_callback=callback)

    channel.basic_qos(prefetch_count=1)

    channel.start_consuming()


if __name__ == '__main__':
    main()






# def job():
    """
    Точка входа в программу.
    1. Получаем лист уникальных идентификаторов всех клиентов
    2. Проходимся по списку всех скриптов.
    """
    # получаем лист из id клиентов
    # clients_list = all_clients_dataframe['mpid'].tolist()
    # _ = 0
    # for mpid in clients_list:
    #     pass
        # nomenclature(mpid, 'wildberries-post')
        # reportdetailesbyperiod(mpid, 'wildberries-get')
        # orders_wb(mpid, 'wildberries-get')
        # stocks_wb(mpid, 'wildberries-get')

        # sales_wb(mpid, 'wildberries-get')
        # sales_daily_wb(mpid, 'wildberries-get')
        # orders_daily_wb(mpid, 'wildberries-get')

    # DecoCount.log_export()


