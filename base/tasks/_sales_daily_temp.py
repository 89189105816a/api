from tasks.helpers.BaseClasses import *
import pandas as pd
from datetime import datetime, timedelta
from tasks.helpers.config_data import InitialData
import time
from decorators.logger_deco import logger_decorate


@logger_decorate
def sales_daily_wb(mpid, interval, key_type='wildberries-get'):
    print('---sales_daily_wb---')
    # time delay configure

    time_delay = interval

    time_delay_list = [datetime.date(datetime.today()) - timedelta(days=x) for x in range(time_delay)]
    time_delay_list_corrected = [day.strftime("%Y-%m-%d") for day in time_delay_list]

    limit = 100000
    fetched_data = True
    _ = 0
    for element in time_delay_list_corrected:
        request_body = InitialData.wb_sales_url + '?dateFrom=' + str(element) + '&flag=1'
        client = Client(mpid, request_body, InitialData.wb_sales_url, key_type)
        client_key = client.get_api_key_from_mpid(key_type)
        table = client.table
        fetched_data = client.fetch_from_api(client_key)
        print(fetched_data)
        if isinstance(fetched_data, int):
            if fetched_data == 429:
                print('---Too many requests flag, proceeding to next date with save---')
                time_delay_list_corrected.append(element)
                print('waiting for extra 20 secs since last request was rejected...')
                time.sleep(20)
                continue
            else:
                print('---Something bad happened---')
                return fetched_data

        _ += 1

        if not fetched_data:
            print('fetched data for this day is null')
            continue

        df = pd.DataFrame(fetched_data)
        df['mpid'] = mpid
        df['date_load'] = str(datetime.today())
        df['barcode'] = df['barcode'].values.astype(str)
        df['table_id'] = table
        df['self_redemption'] = ''
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        df.drop_duplicates(keep='last')
        print(df)
        type_sql = 'append'
        # TODO продажи дублируются ? Сделать тесты констрейна

        query = None
        constraint_name = "unique_srid"
        # sh_flag - нужно ли отправлять данные в google spreadsheets
        sh_flag = False
        insertion_object = InsertionMeta(df, 'Sales_wb', 'sales_wb_new', mpid, query, type_sql, sh_flag)
        return_object = insertion_object.insertion()

        print('sleeping for 5 secs just in case...')
        time.sleep(5)

    if _ == time_delay:
        print('---Количество запросов по дням совпадает с количеством правильно возвращенных пакетов---')
        print('---Успешное завершение---')
        return True
    else:
        print('!!!Количество запросов по дням не совпадает с количеством правильно возвращенных пакетов!!!')
        return False

