
from base.tasks.helpers.BaseClasses import *
import pandas as pd
from datetime import datetime, timedelta
from base.tasks.helpers.config_data import InitialData
from base.decorators.logger_deco import logger_decorate
from base.decorators.fix_decorator import limit_fix_decorator


@logger_decorate
def orders_wb(mpid, key_type):
    print('---orders_wb---')
    base_distance = 80
    dateto = datetime.date(datetime.today())
    datefrom_outer = dateto - timedelta(days=base_distance)
    limit = 100000

    @limit_fix_decorator(20, base_distance)
    def limit_fix_function(datefrom):
        request_body = InitialData.wb_orders_url + '?dateFrom=' + str(datefrom) + '&flag=0'
        client = Client(mpid, request_body, InitialData.wb_sales_url, key_type)
        client_key = client.get_api_key_from_mpid(key_type)
        table = client.table
        fetched_data = client.fetch_from_api(client_key)
        while isinstance(fetched_data, int):
            if fetched_data == 429:
                print('---Too many requests flag, lets wait for a while---')
                print('waiting for extra 60 secs since last request was rejected...')
                time.sleep(60)
                fetched_data = client.fetch_from_api(client_key)
            else:
                print('---Something bad happened---')
                return fetched_data

        df = pd.DataFrame(fetched_data)
        df['mpid'] = mpid
        df['date_load'] = str(datetime.today())
        df['barcode'] = df['barcode'].values.astype(str)
        df['table_id'] = table
        df['self_redemption'] = ''

        return df

    df_full = limit_fix_function(datefrom_outer)
    if isinstance(df_full, int):
        # fetched data вернул код ошибки вместо нормального ответа, мы протащили его через декоратор и выходим отсюда(
        return df_full

    type_sql = 'append'
    query = None
    # sh_flag - нужно ли отправлять данные в google spreadsheets
    sh_flag = False
    insertion_object = InsertionMeta(df_full, 'Orders_WB', 'orders_wb_new', mpid, query, type_sql, sh_flag)
    return_object = insertion_object.insertion()

    return return_object
