
from base.tasks.helpers.BaseClasses import *
import pandas as pd
from datetime import datetime, timedelta
from base.tasks.helpers.config_data import InitialData
from sqlalchemy import text

# TODO функционал нескольких запросов относительно возрастающего rrd_id надо реализовать миксином, а не захардкодить.


def reportdetailesbyperiod(mpid, key_type):

    print(f"reportdetailed script init, user id -> {mpid}")
    dateto = datetime.date(datetime.today())
    datefrom = dateto - timedelta(days=730)
    limit = 10000

    engine = DBHandler().get_engine()
    connection = engine.connect()

    # добавляем в таблицу строку если такого mpid еще нет, если есть просто берем текущее значение mpid
    with connection as connection:
        query = text(f"""
            INSERT INTO rrid_latest (mpid, rrdid) 
            VALUES ('{mpid}', 0)
            ON CONFLICT (mpid) DO NOTHING;

            SELECT rrdid FROM rrid_latest WHERE mpid = '{mpid}';
        """)

        result = connection.execute(query)

    max_rrdid = int(result.fetchone()[0])
    print(max_rrdid)
    connection.close()

    fetched_data = ''
    _ = 0
    # just in case
    latest_rrdid_value = max_rrdid

    while not isinstance(fetched_data, int):
        request_body = InitialData.wb_reportsdetailbyperiod_url + '?dateFrom=' + str(datefrom) + '&limit=' + str(limit)\
                       + '&rrdid=' + str(max_rrdid) + '&dateto=' + str(dateto)
        client = Client(mpid, request_body, InitialData.wb_reportsdetailbyperiod_url, key_type)
        client_key = client.get_api_key_from_mpid(key_type)
        table = client.table
        fetched_data = client.fetch_from_api(client_key)

        if isinstance(fetched_data, int):
            # выход с кодом ошибки
            return fetched_data

        elif fetched_data is None and _ != 0:
            # случай выхода после успешных обработок
            print('---we good! changing local rrdid value---')
            connection = DBConnector.get_psy_connection()
            cursor = connection.cursor()

            sql = f"UPDATE rrid_latest SET rrdid = '{max_rrdid}' WHERE mpid = '{mpid}'"
            try:
                cursor.execute(sql)
                connection.commit()
            except Exception as e:
                print(e)
                return e
            return True
        elif fetched_data is None and _ == 0:
            # мы не получили ошибки, fetched_data пустое -> нечего обновлять, все актуальное
            print('No database insertion. Local rrdid is max rrdid')
            return True

        _ += 1
        df = pd.DataFrame(fetched_data)
        df['mpid'] = mpid
        df['date_load'] = str(datetime.today())
        df['barcode'] = df['barcode'].values.astype(str)
        df['table_id'] = table
        df['shipment'] = ''
        df['self_redemption'] = ''
        # sale_dt2
        df['sale_dt2'] = ''
        try:
            for i in range(len(df)):
                sale_dt = df.at[i, "sale_dt"]
                date_from = df.at[i, "date_from"]
                date_to = df.at[i, "date_to"]
                if sale_dt < date_from:
                    df.loc[i, "sale_dt2"] = date_from
                elif sale_dt > date_to:
                    df.loc[i, "sale_dt2"] = date_to
                else:
                    df.loc[i, "sale_dt2"] = sale_dt
        except Exception as e:
            # sale_dt became NoneType, that's fine
            pass

        latest_rrdid_value = df.iloc[-1]['rrd_id']
        type_sql = 'append'
        query = None
        # sh_flag - нужно ли отправлять данные в google spreadsheets
        sh_flag = False
        insertion_object = InsertionMeta(df, 'РепортЗаПериодВб', 'reportdetailbyperiod_wb', mpid, query, type_sql,
                                             sh_flag)
        insertion_object.insertion()

        max_rrdid = latest_rrdid_value
        print(f"max_rrdid is now {max_rrdid}")


