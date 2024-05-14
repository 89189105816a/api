import json
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import io
import psycopg2
from psycopg2 import Error
import time
import requests


# получаем все таблицы пользователей
#
def load_on_server(connection, engine, tables):

    dateTo = datetime.date(datetime.today())
    dateFrom0 = dateTo - timedelta(days=50)
    dateTo = str(dateTo)
    limit = 100000
    rrdid0 = 0

    try:
        cursor = connection.cursor()
        for i in tables:
            update = tables[i]["update"]
            if update == "True":
                print('load_on server')
                try:

                    ## получение переменных
                    i = str(i)
                    # sheet_id = tables[i]["id"]
                    # url = tables[i]["url"]
                    apiKeyWB = tables[i]["apiKeyWB"]
                    len_apiKeyWB = len(apiKeyWB)
                    name = tables[i]["name"]
                    user_id = int(i) + 1001
                    print(name)
                    print(user_id)
                    # print(url)
                    print(apiKeyWB)
                    # получение переменных

                    cycle = 0
                    while cycle <2:
                        try:
                            try:
                                # запрос dateFrom

                                sql = f"SELECT max(rrd_id) FROM reportdetailbyperiod_wb WHERE user_id = '{user_id}'"

                                cursor.execute(sql)
                                mobile_records = cursor.fetchall()
                                mobile_records = mobile_records[0]
                                mobile_records = mobile_records[0]
                                print(mobile_records)
                            except:
                                mobile_records = None

                            if mobile_records == None:
                                rrdid = rrdid0
                            else:
                                rrdid = int(mobile_records)

                            if len_apiKeyWB < 68:
                                URL = f'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom={dateFrom0}&key={apiKeyWB}&limit={limit}&rrdid={rrdid}&dateto={dateTo}'
                                print(URL)
                                df = pd.read_json(URL)
                            else:
                                headers = {'Authorization': apiKeyWB};
                                URL = requests.get(f"https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom={dateFrom0}&limit={limit}&rrdid={rrdid}&dateto={dateTo}", headers=headers)
                                print(URL)
                                URL = URL.text
                                df = pd.read_json(URL)


                            if len(df) != 0:
                                df['user_id'] = int(i) + 1001
                                df['date_load'] = str(datetime.today())
                                df['barcode'] = df['barcode'].values.astype(str)
                                df['sale_dt2'] = ''
                                df['sale_dt'] = df['sale_dt'].fillna('1970-01-01T00:00:00Z')

                                # исключение .0 в строке barcode
                                cycle = 0
                                index = len(df)
                                while cycle < index:
                                    # print(line)
                                    value = df.at[cycle, "barcode"]

                                    if value[-2:] == '.0':
                                        # print(cycle, ' ', value)
                                        df.loc[cycle, "barcode"] = value[:-2]
                                    # else:
                                        # print(value)

                                    sale_dt = df.at[cycle, "sale_dt"]
                                    date_from = df.at[cycle, "date_from"]
                                    date_to = df.at[cycle, "date_to"]
                                    if sale_dt < date_from:
                                        df.loc[cycle, "sale_dt2"] = date_from
                                    elif sale_dt > date_to:
                                        df.loc[cycle, "sale_dt2"] = date_to
                                    else:
                                        df.loc[cycle, "sale_dt2"] = sale_dt

                                    cycle += 1


                                # def correct_clicks(x):
                                #     if x < 200:
                                #         return x - 50
                                #     else:
                                #         return x - 100
                                # df['sale_dt2'] = df.apply(lambda x: x['sale_dt'], )
                                # настроить повторение
                                df['self_redemption'] = '0'
                                # df = df.loc[df['supplier_oper_name'] == 'Возмещение издержек по перевозке', 'quantity'] = 0
                                df = df[df['supplier_oper_name'].isin(
                                    ['Авансовая оплата за товар без движения', 'Возврат', 'Сторно продаж', 'Логистика',
                                     'Корректная продажа', 'Штрафы', 'Корректный возврат', 'Частичная компенсация брака',
                                     'Сторно возвратов', 'Логистика сторно', 'Оплата брака', 'Продажа',
                                     'Оплата потерянного товара'])]

                                df.to_sql('reportdetailbyperiod_wb', engine, if_exists='append')
                                print('reportdetailbyperiod_wb ok')
                        except Exception as ex:
                            print(ex)
                            if ex == 'HTTP Error 401: Unauthorized':
                                cycle = 5
                            else:

                                time.sleep(10)
                                cycle +=1
                                print('expected exception')
                        else:
                            # break loop
                            break
                    # reportDetailByPeriod?dateFrom


                    # orders
                    cycle = 0
                    while cycle < 2:
                        try:
                        #     sql = f"SELECT max(\"lastChangeDate\") FROM orders_wb WHERE user_id = '{user_id}'"
                        #
                        #     cursor.execute(sql)
                        #     mobile_records = cursor.fetchall()
                        #
                        #     try:
                        #         mobile_records = mobile_records[0]
                        #         mobile_records = mobile_records[0]
                        #     except:
                        #         mobile_records = None
                        #
                        #     if mobile_records != None:
                        #         mobile_records = datetime.strptime(mobile_records, "%Y-%m-%dT%H:%M:%S")
                        #         mobile_records += timedelta(seconds=1)
                        #     try:
                        #         mobile_records = mobile_records.strftime("%Y-%m-%dT%H:%M:%S")
                        #     except:
                        #         mobile_records = None
                        #     print(mobile_records)
                        #
                        #     if mobile_records == None:
                        #         dateFrom = dateFrom0
                        #     else:
                        #         dateFrom = mobile_records


                            if len_apiKeyWB < 68:
                                URL = f'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders?dateFrom={dateFrom0}&flag=0&key={apiKeyWB}'
                                print(URL)
                                df = pd.read_json(URL)
                            else:
                                headers = {'Authorization': apiKeyWB};
                                URL = requests.get(f'https://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={dateFrom0}&flag=0', headers=headers)
                                print(URL)
                                URL = URL.text
                                df = pd.read_json(URL)

                            if len(df) != 0:
                                df['user_id'] = int(i) + 1001
                                df['date_load'] = str(datetime.today())
                                df['name'] = name
                                df['barcode'] = df['barcode'].values.astype(str)

                                # исключение .0 в строке barcode
                                cycle = 0
                                index = len(df)
                                while cycle < index:
                                    # print(line)
                                    value = df.at[cycle, "barcode"]

                                    if value[-2:] == '.0':
                                        # print(cycle, ' ', value)
                                        df.loc[cycle, "barcode"] = value[:-2]
                                    # else:
                                        # print(value)
                                    cycle += 1


                                # запись в бд
                                df['self_redemption'] = '0'
                                df.to_sql('orders_wb', engine, if_exists='append')
                                # df.to_sql('orders_wb_all', engine, if_exists='append')
                                print('orders_wb ok')
                        except Exception as ex:
                            print(ex)
                            if ex == 'HTTP Error 401: Unauthorized':
                                cycle = 5
                            else:

                                time.sleep(10)
                                cycle += 1
                                print('expected exception')
                        else:
                            # break loop
                            break


                    # stocks
                    cycle = 0
                    while cycle < 2:
                        try:

                            if len_apiKeyWB < 68:
                                URL = f'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks?dateFrom={dateFrom0}&key={apiKeyWB}'
                                print(URL)
                                df = pd.read_json(URL)
                            else:
                                headers = {'Authorization': apiKeyWB};
                                URL = requests.get(f'https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={dateFrom0}', headers=headers)
                                print(URL)
                                URL = URL.text
                                df = pd.read_json(URL)

                            if len(df) != 0:
                                df.loc[(df['barcode']) == '', 'barcode'] = 0
                                df['user_id'] = int(i) + 1001
                                df['date_load'] = str(datetime.today())
                                df['name'] = name
                                df['barcode'] = df['barcode'].values.astype(str)

                                # исключение .0 в строке barcode
                                cycle = 0
                                index = len(df)
                                while cycle < index:
                                    # print(line)
                                    value = df.at[cycle, "barcode"]

                                    if value[-2:] == '.0':
                                        # print(cycle, ' ', value)
                                        df.loc[cycle, "barcode"] = value[:-2]
                                    # else:
                                        # print(value)
                                    cycle += 1

                                # if i == str(0):
                                #     try:
                                #         cursor = connection.cursor()
                                #         cursor.execute('TRUNCATE stocks_wb')
                                #         connection.commit()
                                #         print("sql очищено")
                                #     except Exception as ex:
                                #         print(ex)
                                #         print('ошибка очистки')


                                # запись в бд
                                df.to_sql('stocks_wb', engine, if_exists='append')
                                # df.to_sql('stocks_wb_all', engine, if_exists='append')
                                print('stocks_wb ok')
                        except Exception as ex:
                            print(ex)
                            if ex == 'HTTP Error 401: Unauthorized':
                                cycle = 5
                            else:

                                time.sleep(10)
                                cycle += 1
                                print('expected exception')
                        else:
                            # break loop
                            break


                    # sales
                    cycle = 0
                    while cycle < 3:
                        try:
                            # sql = f"SELECT max(\"lastChangeDate\") FROM sales_wb WHERE user_id = '{user_id}'"
                            #
                            # cursor.execute(sql)
                            # mobile_records = cursor.fetchall()
                            # try:
                            #     mobile_records = mobile_records[0]
                            #     mobile_records = mobile_records[0]
                            # except:
                            #     mobile_records = None
                            #
                            # if mobile_records != None:
                            #     mobile_records = datetime.strptime(mobile_records, "%Y-%m-%dT%H:%M:%S")
                            #     mobile_records += timedelta(seconds=1)
                            # try:
                            #     mobile_records = mobile_records.strftime("%Y-%m-%dT%H:%M:%S")
                            # except:
                            #     mobile_records = None
                            # print(mobile_records)



                            if len_apiKeyWB < 68:
                                URL = f'https://suppliers-stats.wildberries.ru/api/v1/supplier/sales?dateFrom={dateFrom0}&flag=0&key={apiKeyWB}'
                                print(URL)
                                df = pd.read_json(URL)
                            else:
                                headers = {'Authorization': apiKeyWB};
                                URL = requests.get(f'https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={dateFrom0}&flag=0', headers=headers)
                                print(URL)
                                URL = URL.text
                                df = pd.read_json(URL)

                            if len(df) != 0:
                                df['user_id'] = int(i) + 1001
                                df['date_load'] = str(datetime.today())
                                df['name'] = name
                                df['barcode'] = df['barcode'].values.astype(str)

                                # исключение .0 в строке barcode
                                cycle = 0
                                index = len(df)
                                while cycle < index:
                                    # print(line)
                                    value = df.at[cycle, "barcode"]

                                    if value[-2:] == '.0':
                                        # print(cycle, ' ', value)
                                        df.loc[cycle, "barcode"] = value[:-2]
                                    # else:
                                        # print(value)
                                    cycle += 1

                                # запись в бд
                                df['self_redemption'] = '0'
                                df.to_sql('sales_wb', engine, if_exists='append')
                                # df.to_sql('sales_wb_all', engine, if_exists='append')
                                print('sales_wb ok')
                        except Exception as ex:
                            print(ex)
                            if ex == 'HTTP Error 401: Unauthorized':
                                cycle = 5
                            else:

                                time.sleep(10)
                                cycle += 1
                                print('expected exception')
                        else:
                            # break loop
                            break



                except Exception as ex:
                    print(ex)
                    print('id error tables', i)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")