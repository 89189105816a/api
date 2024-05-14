from base.tasks.helpers.BaseClasses import *
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from base.tasks.helpers.config_data import InitialData
from base.decorators.logger_deco import logger_decorate


@logger_decorate
def stocks_wb(mpid, key_type):
    print('---stocks_wb---')
    dateto = datetime.date(datetime.today())
    datefrom = dateto - timedelta(days=730)
    limit = 100000

    request_body = InitialData.wb_stocks_url + '?dateFrom=' + str(datefrom)
    client = Client(mpid, request_body, InitialData.wb_stocks_url, key_type)
    client_key = client.get_api_key_from_mpid(key_type)

    if client_key == "":
        # у клиента нет ключа на этот отчет, а значит все норм и ему он просто не нужен.
        print(f'---No key found for stocks for user --> {mpid}')
        return True

    table = client.table
    fetched_data = client.fetch_from_api(client_key)

    while isinstance(fetched_data, int):
        if fetched_data == 429:
            print('---Too many requests flag, lets wait for a while---')
            print('waiting for extra 60 secs since last request was rejected...')
            sleep(60)
            fetched_data = client.fetch_from_api(client_key)
        else:
            print('---Something bad happened---')
            return fetched_data

    df = pd.DataFrame(fetched_data)
    df['mpId'] = mpid
    df['barcode'] = df['barcode'].values.astype(str)
    df['tableId'] = table
    df.rename(columns={'Price': 'price', 'Discount': 'discount'}, inplace=True)
    print(df.columns)
    type_sql = 'append'
    query = f"DELETE FROM orm_stocks_wb WHERE mpid = '{mpid}';"
    # sh_flag - нужно ли отправлять данные в google spreadsheets
    sh_flag = False
    insertion_object = InsertionMeta(df, 'StocksWb', 'orm_stocks_wb', mpid, query, type_sql, sh_flag)
    return_object = insertion_object.insertion()

    return return_object






