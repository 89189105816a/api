from base.tasks.helpers.BaseClasses import *
import pandas as pd
import time
from base.tasks.helpers.config_data import InitialData


def nomenclature(mpid, key_type):
    print(f"nomenclature wb script init, user id -> {mpid}")
    request_body = InitialData.wildberries_request_body
    client = Client(mpid, request_body, InitialData.wildberries_request_url, key_type)
    # объект класса client взаимодействует с со всеми классами своими методами
    # объект fetched_data содержит или 'Error fetching API data' или тело ответа от API по id текущего клиента
    client_key = client.get_api_key_from_mpid(key_type)
    print(client_key)
    fetched_data = client.fetch_from_api(client_key)
    table = client.table
    # внутри spreadsheet_data пара id/url для клиента с нашим mpid
    spreadsheet_data = client.get_spreadsheet_id_for_client_id()

    complete_dataframe = pd.DataFrame()

    while isinstance(fetched_data, int):
        if fetched_data == 429:
            print('---Too many requests flag, lets wait for a while---')
            print('waiting for extra 60 secs since last request was rejected...')
            time.sleep(60)
            fetched_data = client.fetch_from_api(client_key)
        else:
            print('---Something bad happened---')
            return fetched_data

    complete_dataframe = do_update(mpid, fetched_data, table, client.alldata)
    complete_dataframe.drop_duplicates()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    print(complete_dataframe)
    print(f"Начинаем вставку данных по номенклатуре для пользователя {mpid}")
    # return insertion(mpid, complete_dataframe, spreadsheet_data)


def do_update(mpid, fetched_data, table, alldata):
    # подготовка данных для вставки

    fetched_result = fetched_data['data']['cards']
    df = pd.DataFrame(fetched_result)
    temp_data = []

    # нужен для преобразования {text1, text2} в чистый text1, text2

    def join_values(lst):
        return ', '.join(map(str, lst))

    # разворачиваем sizes
    for index, row in df.iterrows():
        field_list = row['sizes']
        for item in field_list:
            new_row = item.copy()
            new_row.update(row.drop('sizes').to_dict())
            temp_data.append(new_row)

    new_df = pd.DataFrame(temp_data)

    new_df['table_id'] = [table] * len(new_df)
    new_df['mpid'] = [mpid] * len(new_df)
    new_df['skus'] = new_df['skus'].apply(join_values)
    new_df['colors'] = new_df['colors'].apply(join_values)
    new_df['mediaFiles'] = new_df['mediaFiles'].apply(lambda x: x[0] if x else None)

    # в датафрейме много лишнего, оставляем только самое нужное
    new_df = new_df[['mpid', 'techSize', 'updateAt', 'vendorCode', 'brand', 'object', 'nmID', 'table_id', 'skus',
                     'colors', 'mediaFiles']]

    # тут датафрейм правильный, но в нем отсутствуют данные из смежных таблиц. Добавляем.
    # TODO наверное можно найти более быстрый способ мержа, пока нет времени.

    """ НА ВРЕМЯ ТЕСТОВ БЕЗ ДОНОРОВ """
    # donor_dataframes = nomenclature_from_tables(table)
    #
    # unique_barcodes_from_donor = donor_dataframes['report'][~donor_dataframes['report']['skus'].isin(new_df['skus'])]
    # result_df = pd.concat([new_df, unique_barcodes_from_donor], ignore_index=True)
    # new_df = result_df
    #
    # unique_barcodes_from_donor = donor_dataframes['orders'][~donor_dataframes['orders']['skus'].isin(new_df['skus'])]
    # result_df = pd.concat([new_df, unique_barcodes_from_donor], ignore_index=True)
    # new_df = result_df
    #
    # unique_barcodes_from_donor = donor_dataframes['sales'][~donor_dataframes['sales']['skus'].isin(new_df['skus'])]
    # result_df = pd.concat([new_df, unique_barcodes_from_donor], ignore_index=True)
    # new_df = result_df

    # добавляем link и cost_price

    new_df['link'] = 'https://www.wildberries.ru/catalog/' + new_df['nmID'].astype(str) + '/detail.aspx?targetUrl=LC'
    # объект класса SPHandler в методе from_dataframe_for_nomenclature возвращает по table датафрейм пар
    # skus -> costprice
    spreadsheet_object = SPHandler(alldata)
    dataframe_with_cost_price = spreadsheet_object.form_dataframe_for_nomenclature()
    full_nomenclature_dataframe = new_df.merge(dataframe_with_cost_price, on='skus', how='left')

    # int данные становятся, где-то, float. TODO найти где и исправить нормально
    full_nomenclature_dataframe['updateAt'] = full_nomenclature_dataframe['updateAt'].astype(str).str.rstrip('.0')
    full_nomenclature_dataframe['skus'] = full_nomenclature_dataframe['skus'].astype(str).str.rstrip('.0')
    full_nomenclature_dataframe['mpid'] = full_nomenclature_dataframe['mpid'].astype(str).str.rstrip('.0')
    full_nomenclature_dataframe['table_id'] = full_nomenclature_dataframe['table_id'].astype(str).str.rstrip('.0')
    full_nomenclature_dataframe = full_nomenclature_dataframe.fillna('')

    # если у нас отсутсвуют mediaFiles и link

    # the script needs mpid to be present, so if not - getting rid of it
    full_nomenclature_dataframe = full_nomenclature_dataframe[full_nomenclature_dataframe['mpid'] != 'nan']
    return full_nomenclature_dataframe


def insertion(mpid, dataframe, spreadsheet_data):
    # меняем порядок столбцов датафрейма чтобы было красивее вывод в спредшит
    new_dataframe_order = ['mpid', 'table_id', 'nmID', 'skus', 'vendorCode', 'brand', 'object', 'colors',
                           'cost_price', 'link', 'mediaFiles', 'updateAt']
    dataframe = dataframe[new_dataframe_order]

    # добавляем конфиг для вставки
    query = f"DELETE FROM nomenclature_wb WHERE mpid = '{mpid}';"
    type_sql = 'append'
    insertion_object = InsertionMeta(dataframe, 'Номенклатура', 'nomenclature_wb', mpid, query,
                                     type_sql, False, spreadsheet_data)
    return_object = insertion_object.insertion()

    return return_object
