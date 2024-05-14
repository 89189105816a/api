import requests
import json
import pandas as pd
import io
import gspread
from sqlalchemy import create_engine
from datetime import datetime
import time
import numpy as np
from urllib.parse import quote_plus

with io.open("tables.json", encoding='utf-8', mode='r') as json_file:
  tables = json.load(json_file)
gc = gspread.service_account(filename="credentials.json")
with io.open("client.json", encoding='utf-8', mode='r') as json_file:
    client = json.load(json_file)

for l in client:
    try:
        l = str(int(l))
        # print(l)
        All_tables = client[l]["tables"]
        All_tables = All_tables.split(',')
        print(All_tables)
        sheet_id = client[l]["id"]
        url = client[l]["url"]


        offer_id = []
        sku = []
        title = []
        category = []
        discounted = []
        barcode = []
        length = []
        width = []
        height = []
        volume = []
        weight = []
        not_for_sale = []
        loss = []
        for_sale = []
        between_warehouses = []
        shipped = []
        user_id_df = []


        for t in All_tables:
            try:
                # конвектирование user_id в id json
                  t = str(int(t) - 1001)
                  user_id = tables[t]["table"]
                  print(user_id)
                  clientId = tables[t]["Client_ID"]
                  apiKey = tables[t]["Api_OZON"]

                  headers = {
                    'Client-Id': clientId,
                    'Api-Key': apiKey
                  };
                  body = {
                  "limit": 1000,
                  "offset": 0
                  };



                  body = json.dumps(body)
                  print(18)


                  response = requests.post("https://api-seller.ozon.ru/v1/analytics/stock_on_warehouses", headers=headers, data=body)

                  response = response.text
                  response = response
                  response = json.loads(response)
                  # print(body)
                  response = response['total_items']
                  #
                  len_response = len(response)
                  print(len_response)
                  if len_response == 0:
                      break

                  j = 1
                  # k = 0
                  for i in response:
                    try:
                      barcode_l = i['barcode'].split(';')
                      for j in barcode_l:

                        # print(type(barcode_l))
                        # print(barcode_l)
                        offer_id += [i['offer_id']]
                        sku += [i['sku']]
                        title += [i['title']]
                        category += [i['category']]
                        discounted += [i['discounted']]
                        barcode += [j]
                        length += [i['length']]
                        width += [i['width']]
                        height += [i['height']]
                        volume += [i['volume']]
                        weight += [i['weight']]
                        not_for_sale += [i['stock']['not_for_sale']]
                        loss += [i['stock']['loss']]
                        for_sale += [i['stock']['for_sale']]
                        between_warehouses += [i['stock']['between_warehouses']]
                        shipped += [i['stock']['shipped']]
                        user_id_df += [user_id]

                    except:
                      offer_id += [i['offer_id']]
                      sku += [i['sku']]
                      title += [i['title']]
                      category += [i['category']]
                      discounted += [i['discounted']]
                      barcode += [i['barcode']]
                      length += [i['length']]
                      width += [i['width']]
                      height += [i['height']]
                      volume += [i['volume']]
                      weight += [i['weight']]
                      not_for_sale += [i['stock']['not_for_sale']]
                      loss += [i['stock']['loss']]
                      for_sale += [i['stock']['for_sale']]
                      between_warehouses += [i['stock']['between_warehouses']]
                      shipped += [i['stock']['shipped']]
                      user_id_df += [user_id]









            except Exception as ex:
                print(ex)
                print('error ',user_id)



        data = pd.DataFrame({
            "offer_id": offer_id,
            "sku": sku,
            "title": title,
            "category": category,
            "discounted": discounted,
            "barcode": barcode,
            "length": length,
            "width": width,
            "height": height,
            "volume": volume,
            "weight": weight,
            "not_for_sale": not_for_sale,
            "loss": loss,
            "for_sale": for_sale,
            "between_warehouses": between_warehouses,
            "shipped": shipped,
            "user_id": user_id_df
        })
        # print(vals)
        data['date_load'] = str(datetime.today())

        # подключение к гкгл таблицам
        sh = gc.open_by_key(sheet_id)
        worksheet = sh.worksheet("Склад (исх.) (ОЗОН)")

        # получение себеса
        ws_values = worksheet.get_all_values()
        try:
            df = pd.DataFrame.from_records(ws_values[1:], columns=ws_values[0])

            df = df.rename(columns={'Себестоимость': 'cost_price'})

            df = df[["sku", "cost_price"]]


            df = df[df['sku'] != '']
            df['sku'] = df['sku'].values.astype('int64')
            # print(df.dtypes)
            # print(data.dtypes)
            data = data.merge(df, how='left', on='sku')

            data['sku'] = data['sku'].values.astype('object')
        except:
            print("нет себеса")
        data = data.drop_duplicates()
        # запись в бд
        engine = create_engine('postgresql://postgres:TrW34Uq@localhost:5432/dashboard3')

        data.to_sql('analytics_stock_on_warehouses_ozon', engine, if_exists='append')
        # data.to_sql('analytics_stock_on_warehouses_ozon_all', engine, if_exists='append')
        print('обновлено ', t)

        # запись в гугл таблицы

        data = data.fillna('')
        # print(153)
        values = data.values
        values = values.tolist()
        # print(data)
        # print(type(values))
        len_values = len(values)
        print('len_values', len_values)

        if len_values != 0:
            cycle = 0
            while cycle < 3:
                try:
                    cycle += 1
                    worksheet.batch_clear(["A2:S20000"])
                    worksheet.update(f'A2:S{len_values + 1}', values)

                    print("data upgrage")

                # запись даты обновления
                # worksheet = sh.worksheet("обновление таблиц")
                # worksheet.update(f'B10', date_now)

                except Exception as ex:
                    print(ex)
                    time.sleep(10)
                else:
                    # break loop
                    break
        time.sleep(10)
    except Exception as ex:
        print(ex)
        print('error ', l)

