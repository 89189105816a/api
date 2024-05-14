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

"чтение json с таблицами"
with io.open("tables.json", encoding='utf-8', mode='r') as json_file:
    tables = json.load(json_file)
    # gc = gspread.service_account(filename="credentials.json")

dateTo = datetime.date(datetime.today())
dateFrom0 = dateTo - timedelta(days=730)
dateTo = str(dateTo)
limit = 100000
rrdid = 0
engine = create_engine('postgresql://postgres:TrW34Uq@localhost:5432/dashboard')


connection = psycopg2.connect(
    user="postgres",
    # пароль, который указали при установке PostgreSQL
    password="TrW34Uq",
    host="localhost",
    port="5432",
    database="dashboard")

try:
    cursor = connection.cursor()
    print(tables)
    for i in tables:
        df_all = pd.DataFrame({})
        rrdid = 0
        update = tables[i]["update"]
        if update == "True":
            print('reportDetailByPeriod')
            try:

                ## получение переменных
                i = str(i)
                sheet_id = tables[i]["id"]
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

                try:

                    headers = {'Authorization': apiKeyWB};
                    while True:
                        URL = requests.get(f"https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod?dateFrom={dateFrom0}&limit={limit}&rrdid={rrdid}&dateto={dateTo}", headers=headers)
                        print(user_id)
                        print(URL)
                        try:
                            URL = URL.text
                            df = pd.read_json(URL)
                            len_df = len(df)
                            if len_df == 0:
                                break




                            df['user_id'] = int(i) + 1001
                            df['date_load'] = str(datetime.today())
                            df['barcode'] = df['barcode'].values.astype(str)
                            df['sale_dt2'] = ''

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
                        except Exception as ex:
                            print(ex)

                            print('ошибка обработки данных')

                        df_all = pd.concat([df_all, df], sort=False, axis=0)
                        rrdid += int(df.loc[cycle - 1, "rrd_id"])
                    print(len(df_all))
                    df_all.to_sql('reportdetailbyperiod_wb', engine, if_exists='append')
                    print('reportdetailbyperiod_wb ok')
                except Exception as ex:
                    print(ex)

                    print('expected exception')

                # reportDetailByPeriod?dateFrom

            except Exception as ex:
                print(ex)
                print('id error tables', i)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")