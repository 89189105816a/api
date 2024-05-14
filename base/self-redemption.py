import json
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, select
import numpy as np
import io
import gspread
import psycopg2
from psycopg2 import Error

"чтение json с таблицами"
with io.open("depricated/tables.json", encoding='utf-8', mode='r') as json_file:
    tables = json.load(json_file)
    gc = gspread.service_account(filename="depricated/credentials.json")
with io.open("depricated/client.json", encoding='utf-8', mode='r') as json_file:
    client = json.load(json_file)

engine = create_engine('postgresql://postgres:TrW34Uq@localhost:5432/dashboard3')
conn = psycopg2.connect(
    user="postgres",
    # пароль, который указали при установке PostgreSQL
    password="TrW34Uq",
    host="localhost",
    port="5432",
    database="dashboard")

for l in client:
    l = str(int(l))
    # print(l)
    All_tables = client[l]["tables"]
    All_tables = All_tables.split(',')

    sheet_id = client[l]["id"]
    url = client[l]["url"]
    client_name = client[l]["client"]
    print(url)
    print(client_name)


    # получить данные srid из гугла
    gc = gspread.service_account(filename="depricated/credentials.json")
    sh = gc.open_by_key(sheet_id)
    if client_name == 'Эльмир':
        print('Самовыкупы1')
        worksheet = sh.worksheet("Самовыкупы1")
        ws_values = worksheet.get_all_values()
        df_GS = pd.DataFrame.from_records(ws_values[1:], columns=ws_values[0])
        df_GS = df_GS.iloc[:, 0:13]
        worksheet = sh.worksheet("Самовыкупы")
    else:
        worksheet = sh.worksheet("Самовыкупы")
        ws_values = worksheet.get_all_values()
        df_GS = pd.DataFrame.from_records(ws_values[1:], columns=ws_values[0])
        df_GS = df_GS.iloc[:, 0:13]
    # print(df_GS.dtypes)
    df_GS.columns = ['srid','srid2', 'barcode', 'nm_id', 'sa_name', 'ts_name', 'retail_amount', 'order_dt', 'sale_rfs', 'sale_dt', 'count', 'realizationreport_id']
    # print(df_GS)
    # получить данные
    srid1 = df_GS['srid'].values
    srid1 = srid1.tolist()
    srid2 = df_GS['srid2'].values
    srid2 = srid2.tolist()
    # print(srid2)



    len_srid = len(srid1)
    i = 0
    srid = []
    while i < len_srid:
        if srid2[i][:3] == 'htt'  or srid2[i] == '':
            srid += [srid1[i]]
        else:
            srid += [srid2[i]]
        i += 1
    # print(srid)



    index_df_GS = len(df_GS)


    df = pd.DataFrame({
        "srid": [],
        # "barcode": [],
        # "nm_id": [],
        # "sa_name": [],
        # "ts_name": [],
        # "retail_amount": [],
        # "order_dt": [],
        # "sale_rfs": [],
        # "sale_dt": [],
        # "count": []
    })
    df["srid"] = srid
    df_new = pd.DataFrame({})
    df_srid_sales = pd.DataFrame({})
    df_srid_order = pd.DataFrame({})
    for n in All_tables:
        # Запись данных о самовыкупе в базу
        try:
            loop = 0
            print(len_srid)
            while True:

                srid_fdb = srid[0 + loop : 1000 + loop]
                if len(srid_fdb) == 0:
                    break
                srid_fdb = str(srid_fdb)
                srid_fdb = srid_fdb[1:-1]
                # print(srid_fdb)
                cursor = conn.cursor()

                cursor.execute(f'''
                                UPDATE reportdetailbyperiod_wb
                                SET self_redemption = '1'
                                WHERE user_id = '{n}' and srid in ({srid_fdb})
                                ''')
                conn.commit()
                # print(f'''
                #                 UPDATE reportdetailbyperiod_wb
                #                 SET self_redemption = '1'
                #                 WHERE user_id = '{n}' and srid in ({srid_fdb})
                #                 ''')
                cursor.execute(f'''
                                UPDATE sales_wb
                                SET self_redemption = '1'
                                WHERE user_id = '{n}' and srid in ({srid_fdb})
                                ''')
                conn.commit()
                cursor.execute(f'''
                                UPDATE orders_wb
                                SET self_redemption = '1'
                                WHERE user_id = '{n}' and srid in ({srid_fdb})
                                ''')
                conn.commit()
                # # добавить 0 в столбец
                # cursor.execute(f'''
                #                 UPDATE reportdetailbyperiod_wb
                #                 SET self_redemption = '0'
                #                 WHERE user_id = '{n}' and srid NOT IN ({srid_fdb})
                #                 ''')
                # conn.commit()
                # cursor.execute(f'''
                #                 UPDATE sales_wb
                #                 SET self_redemption = '0'
                #                 WHERE user_id = '{n}' and srid NOT IN ({srid_fdb})
                #                 ''')
                # conn.commit()
                # cursor.execute(f'''
                #                 UPDATE orders_wb
                #                 SET self_redemption = '0'
                #                 WHERE user_id = '{n}' and srid NOT IN ({srid_fdb})
                #                 ''')
                # conn.commit()
                loop += 1000
                print(loop, ' из ', len_srid)
                conn.commit()
            print('Самовыкупы в ДБ загружены')
        except (Exception, Error) as ex:
            print(ex)
            print('ошибка при загрузке ДБ')

        i = 0
        sql = f"select reportdetailbyperiod_wb.realizationreport_id, reportdetailbyperiod_wb.srid, reportdetailbyperiod_wb.barcode, reportdetailbyperiod_wb.nm_id, reportdetailbyperiod_wb.sa_name, reportdetailbyperiod_wb.ts_name, reportdetailbyperiod_wb.retail_amount, date(reportdetailbyperiod_wb.sale_dt) as sale_dt FROM reportdetailbyperiod_wb WHERE doc_type_name = 'Продажа' and reportdetailbyperiod_wb.user_id = '{n}' and retail_amount != 0"
        interim = pd.read_sql(sql, con=engine)
        df_new = pd.concat([df_new, interim], sort=False,axis=0)
        # print(df_new)
        # print(df_new)


        sql = f"select srid, barcode, \"nmId\" as nm_id, \"supplierArticle\" as sa_name, \"techSize\" as ts_name, \"finishedPrice\" as retail_amount, \"discountPercent\", date(date) as sale_rfs FROM sales_wb WHERE sales_wb.user_id = '{n}'"
        interim = pd.read_sql(sql, con=engine)
        df_srid_sales = pd.concat([df_srid_sales, interim], sort=False,axis=0)


        sql = f"select srid, barcode, \"nmId\" as nm_id, \"supplierArticle\" as sa_name, \"techSize\" as ts_name, \"totalPrice\", \"discountPercent\", date(orders_wb.date) as order_dt FROM orders_wb WHERE orders_wb.user_id = '{n}'"
        interim = pd.read_sql(sql, con=engine)
        df_srid_order = pd.concat([df_srid_order, interim], sort=False,axis=0)


    # df_srid_sales['retail_amount'] = df_srid_sales['totalPrice'] - (
    #             df_srid_sales['totalPrice'] * df_srid_sales['discountPercent'] * 0.01)
    df_srid_order['retail_amount'] = df_srid_order['totalPrice'] - (
                df_srid_order['totalPrice'] * df_srid_order['discountPercent'] * 0.01)
    df_srid_order['retail_amount'] = df_srid_order['retail_amount'].round(2)

    df_new = df.merge(df_new, left_on='srid', right_on='srid', how='left')
    df_new = df_new[df_new['srid'] != '']
    df_new = df_new.drop_duplicates(subset=['srid'])
    df_new.set_index('srid', inplace=True)

    df_srid_order = df.merge(df_srid_order, left_on='srid', right_on='srid', how='left')
    df_srid_order = df_srid_order[df_srid_order['srid'] != '']
    df_srid_order = df_srid_order.drop_duplicates(subset=['srid'])
    df_srid_order.set_index('srid', inplace=True)

    df_srid_sales = df.merge(df_srid_sales, left_on='srid', right_on='srid',how='left')
    df_srid_sales = df_srid_sales[df_srid_sales['srid'] != '']
    df_srid_sales = df_srid_sales.drop_duplicates(subset=['srid'])
    df_srid_sales.set_index('srid', inplace=True)
    # print(df_srid_order[['retail_amount', 'totalPrice']])

    df = pd.DataFrame({
        "srid": [],
        "barcode": [],
        "nm_id": [],
        "sa_name": [],
        "ts_name": [],
        "retail_amount": [],
        "order_dt": [],
        "sale_rfs": [],
        "sale_dt": [],
        "count": [],
        "realizationreport_id": []
    })
    df["srid"] = srid
    # print(len(df))
    df = df.drop_duplicates(subset=['srid'])
    df.set_index('srid', inplace=True)
    # print(len(df))
    # print(df)
    # print(df_new)
    df = df.fillna(value=df_new)
    df = df.fillna(value=df_srid_sales)
    df = df.fillna(value=df_srid_order)
    df['count'] = 1
    # print(104,df)
    df = df.reset_index()
    df['srid2'] = ""
    # print(df)
    df = df[['srid','srid2','barcode', 'nm_id', 'sa_name', 'ts_name','retail_amount','order_dt','sale_rfs','sale_dt', "count", 'realizationreport_id']]
    # df.to_csv('1.csv')
    # df = df.drop(columns=['srid'])
    # df.to_csv('2.csv')
    # print(106, df)
    # df.to_csv('1.csv')
    # print(df)

    index = len(df)
    cycle = 0
    df['order_dt'] = df['order_dt'].values.astype('str')
    df['sale_dt'] = df['sale_dt'].values.astype('str')
    df['sale_rfs'] = df['sale_rfs'].values.astype('str')
    df = df.fillna('')
    df = df.replace(['nan'], '')


    values = df.values
    values = values.tolist()
    len_values = len(values)
    worksheet = sh.worksheet("Самовыкупы")
    worksheet.batch_clear(["A2:L100000"])
    worksheet.update(f'A2:L{len_values + 1}', values)





