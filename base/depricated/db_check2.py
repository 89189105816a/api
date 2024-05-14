from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import io
import gspread
import json
from datetime import datetime
import time
import schedule

date_now = str(datetime.today())
with io.open("tables.json", encoding='utf-8', mode='r') as json_file:
  tables = json.load(json_file)
  gc = gspread.service_account(filename="credentials.json")

engine = create_engine('postgresql://postgres:TrW34Uq@localhost:5432/dashboard3')
connection = psycopg2.connect(
    user="postgres",
    # пароль, который указали при установке PostgreSQL
    password="TrW34Uq",
    host="localhost",
    port="5432",
    database="dashboard")
db_all = ['analytics_stock_on_warehouses_ozon', 'finance_transaction_list_ozon', 'nomenclature_wb', 'orders_wb', 'posting_fbo_list_ozon', 'posting_fbs_list_ozon', 'reportdetailbyperiod_wb', 'sales_wb', 'stocks_wb']

def db_check():
    for i in tables:
        table = tables[i]["table"]
        #update = tables[i]["update"]
        update = "True"
        if update == "True":

            try:

                for db in db_all:
                    try:
                        sql = f"select * FROM {db} where user_id = '{table}'"
                        df = pd.read_sql(sql, con=engine)
                        if len(df) == 0:
                            sql = f"select * FROM {db + '_all'} where date_load = (SELECT max(date_load) from {db + '_all'} where user_id = '{table}') and user_id = '{table}'"
                            df = pd.read_sql(sql, con=engine, index_col="index")
                            df.to_sql(db, engine, if_exists='append')

                        # if db == 'nomenclature_wb':
                        #     # загрузка в гугл док
                        #     sheet_id = tables[i]["table"]
                        #     sh = gc.open_by_key(sheet_id)
                        #
                        #     worksheet = sh.worksheet("Номенклатура")
                        #     ws_values = worksheet.get_all_values()
                        #     data['skus'] = data['skus'].values.astype('object')
                        #     # data['cost_price'] = data['cost_price'].values.astype('number')
                        #     data = data.fillna('')
                        #     # print(153)
                        #     values = data.values
                        #     values = values.tolist()
                        #     # print(values)
                        #     # print(type(values))
                        #     len_values = len(values)
                        #     print(len_values)
                        #
                        #     if len_values != 0:
                        #         cycle = 0
                        #         while cycle < 5:
                        #             try:
                        #                 cycle += 1
                        #                 worksheet.batch_clear(["B2:M20000"])
                        #                 worksheet.update(f'B2:M{len_values + 1}', values)
                        #                 # print(df.dtypes)
                        #                 print("data upgrage")
                        #                 # print(vals)
                        #
                        #
                        #             except Exception as ex:
                        #                 print(ex)
                        #                 time.sleep(10)
                        #             else:
                        #                 # break loop
                        #                 break
                        print('db_check in db: ', db, ' ', date_now)
                    except Exception as ex:
                        print(ex)
                        print('ошибка db_check in db: ', db, ' ', date_now)

            except Exception as ex:
                print(ex)
                print('ошибка db_check in tables: ', table, ' ', date_now)

# schedule.every(2).hours.at("08:20").do(db_check)
schedule.every().day.at("06:00").do(db_check)
schedule.every().day.at("17:00").do(db_check)
schedule.every().day.at("23:00").do(db_check)

while True:
    schedule.run_pending()
    time.sleep(1)