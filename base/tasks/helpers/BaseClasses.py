
# Файл со вспомогательными классами.

import requests
import json
import pandas as pd
import gspread
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from cryptography.fernet import Fernet
import psycopg2
import os
from decorators.proxy_manager_deco import use_proxy
from dotenv import load_dotenv
from tasks.helpers.config_data import InitialData

load_dotenv('.back_env')

key = os.getenv('1a3f0b9e4c8d2h6j5k7m9p0r2t5w7y')


class DBConnector:
    """
    Класс DBConnector используется для получения объектов engine/connection
    """
    def __init__(self, user, password, host, port, database):
        """

        :param user: database user
        :param password: database pass
        :param host: host addr
        :param port: standart port for postgres
        :param database: database name

        :return engine

        """
        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}', echo=False)

    # получаем connection для коротких запросов
    @staticmethod
    def get_psy_connection():
        """
        Метод получения объекта connection

        :return: объект connection
        """
        connection = psycopg2.connect(
            user=os.getenv('DB_USER'),
            # пароль, который указали при установке PostgreSQL
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_ADDR'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_DATABASE_MAIN'))

        return connection


class DBHandler:
    """
    Класс работы с базой. Все операции с базой реализованы как методы этого класса.

    Methods
    -------
    get_engine()
        возвращает объект engine
    get_all_clients()
        возвращает датафрейм со всеми данными всех клиентов из таблицы clients_general.
        Используется для прохода по всем клиентам
    get_all_data_for_mpid(mpid)
        возвращает строку из базы с нужным идентификатором клиента в виде датафрейма

    """

    def __init__(self):
        self.engine = DBConnector(os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_ADDR'),
                                  os.getenv('DB_PORT'), os.getenv('DB_DATABASE_MAIN')).engine

    def get_engine(self, database='db3'):
        if database == 'db3':
            return DBConnector(os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_ADDR'),
                                  os.getenv('DB_PORT'), os.getenv('DB_DATABASE_MAIN')).engine
        else:
            return self.engine

    def get_all_clients(self):
        db_dataframe_clients = pd.read_sql_query("SELECT * FROM clients_general ORDER BY mpid", self.engine)
        return db_dataframe_clients

    def get_all_data_for_mpid(self, mpid):
        db_dataframe_response = pd.read_sql_query(f"SELECT * FROM clients_general WHERE mpID = {mpid}", self.engine)
        return db_dataframe_response


class ProxyManager:

    @staticmethod
    def request_wrapper(request_type, *args, **kwargs):
        pass


class APIhandle:
    """
    Класс работы с API

    Methods
    -------
    data_fetch()
        Выполняет запрос к API на основании урла, хедеров, post/get и API ключа
        возвращает словарь c ответом и доп. информацией, из которой можно подтянуть код ответа и флаг наличия ошибки.
    """

    def __init__(self, api_key: str, body, url, request_type):
        self.api_key = api_key
        self.body = body
        self.url = url
        self.request_type = request_type
        self.header = {"Authorization": f"{self.api_key}"}

    @use_proxy
    def data_fetch(self):
        print('---trying to fetch data---')
        proxy_manager_addr = InitialData.proxy_manager_addr
        data = {
            "api_key": self.api_key,
            "body": self.body,
            "url": self.url,
            "request_type": self.request_type,
            "header": self.header,
        }

        if 'get' in self.request_type:
            print('---using GET request---')
            local_response_object = requests.get(self.body, headers=self.header)
        else:
            print('---using POST request---')
            local_response_object = requests.post(self.url, headers=self.header, data=self.body)

        # print(local_response_object.text, local_response_object.status_code)
        return local_response_object.text, local_response_object.status_code


class Client:
    """
    Класс Client - служит "точкой входа". Всё через методы этого класса, которые в свою очередь
    создают объекты других классов, инкапсулируя в человеко-понятные названия методов всю работу.

    Methods
    -------
    get_spreadsheet_data_from_client_id(mpid)
        Получение кредов клиента google spreadsheet. Не в init т.к. используется нечасто.
    fetch_from_api(api_key)
        Оболочка над фетчем по API, получает необходимый типу запроса ключ(устаревшее, сейчас везде same key),
        делает с помощью класса APIhandle запрос по api, подгоняет данные в нужный формат с помощью манипулятора.
    get_api_key_from_mpid(key-type)
        Устаревшее. Получает свой ключ в зависимости от типа.
    """
    def __init__(self, mpid: int, request_body, api_url, request_type):

        self.mpid = mpid
        self.request_body = request_body
        self.api_url = api_url
        self.request_type = request_type
        self.alldata = DBHandler().get_all_data_for_mpid(mpid)
        self.table = str(self.alldata['table_id'].iloc[0])

    def get_spreadsheet_id_for_client_id(self):
        return {'id': self.alldata['spreadsheet_id'].iloc[0]}

    def fetch_from_api(self, api_key):
        """
        Parameters
        ----------
        api_key

        Returns
        -------
        status code if something is wrong and just json fetched data otherwise
        """
        apihandle_object = APIhandle(api_key, self.request_body, self.api_url, self.request_type)
        apihandle_data = apihandle_object.data_fetch()

        # apihandle_data[0] -> fetched data, apihandle_data[1] -> status code

        if apihandle_data[1] == 200:
            fetched_data = json.loads(apihandle_data[0])
            print(fetched_data)
            return fetched_data
        else:
            print('---we\'ve cought an error fetching data---')
            print(f'---{apihandle_data[1]}')
            return apihandle_data[1]

    def get_api_key_from_mpid(self, key_type):
        if key_type == 'wildberries-get' or key_type == 'wildberries-post':
            return self.alldata['wb_api'].iloc[0]
        else:
            return None


class Crypto:
    """
    Класс для шифрования API ключей.
    """
    @staticmethod
    def encrypt_token(token, key):
        cipher_suite = Fernet(key)
        encrypted_token = cipher_suite.encrypt(token.encode())
        return encrypted_token

    @staticmethod
    def decrypt_token(encrypted_token, key):
        cipher_suite = Fernet(key)
        decrypted_token = cipher_suite.decrypt(encrypted_token).decode()
        return decrypted_token


class SPHandler:
    """
    Класс работы с google spreadsheets

    Methods
    -------
    get_sh_auth_data()
        Возвращает id спредшита клиента
    form_dataframe_for_nomenclature()
        Возвращает dataframe вида [sku(уникальный идентификатор товара), cost_price]. Часть клиентов заполняют
        cost_price именно в гугл таблице, а без этой информации отчет по номенклатуре будет не полным
    return_worksheet_object()
        Возвращает объект worksheet для работы с таблицами
    """

    def __init__(self, alldata):
        # креды сервис-аккаунта
        self.gc = gspread.service_account(filename='../../depricated/credentials.json')
        self.alldata = alldata

    def get_sh_auth_data(self):
        return self.alldata['spreadsheet_id'].iloc[0]

    def form_dataframe_for_nomenclature(self):
        spreadsheet = self.gc.open_by_key(self.alldata['spreadsheet_id'].iloc[0])
        worksheet = spreadsheet.worksheet("Номенклатура")
        ws_values = worksheet.get_all_values()
        worksheet_dataframe = pd.DataFrame.from_records(ws_values[1:], columns=ws_values[0])
        worksheet_dataframe = worksheet_dataframe[["skus", "cost_price"]]
        return worksheet_dataframe

    def return_worksheet_object(self, sheet_id, sheet_name):
        sh = self.gc.open_by_key(sheet_id)
        worksheet = sh.worksheet(sheet_name)
        return worksheet


class InsertionMeta:
    """
    Так как вставка в базу и в гугл таблицы происходит везде примерно одинаковым образом она объединена в класс
    InsertionMeta. Таким образом, чтобы опустить данные в базу или таблицу достаточно единожды создать объект этого
    класса, и выполнить соответствующий метод.

    Methods
    -------
    insertion()
        Непосредственно вставка в базу или в таблицы данных.
    """

    def __init__(self, insertion_object, insertion_info, insertion_table, mpid, query, type_sql, sh_flag, spreadsheet_data=None):
        """
        :param insertion_object: объект вставки в виде датафрейма pandas
        :param insertion_info: название скрипта для логгера
        :param insertion_table: название таблицы в базе
        :param mpid: уникальный идентификатор клиента
        :param query: запрос, выполняемый до вставки. Это может быть удаление строк в которых mpid == mpid клиента
        :param type_sql: append, truncate, replace etc.
        :param sh_flag: флаг необъодимости обновления данных в google таблице, нужно не всегда
        :param spreadsheet_data: креды аккаунта, по умолчанию None т.к. нужно не всегда
        """
        self.insertion_object = insertion_object
        self.insertion_info = insertion_info
        self.insertion_table = insertion_table
        self.type_sql = type_sql
        # query содержит запрос, который выполняется перед скриптом, может быть None. Служит для выполнения запросов на
        # очистку базы если нужно.
        self.query = query
        self.mpid = mpid
        self.sh_flag = False
        self.spreadsheet_data = spreadsheet_data

    def insertion(self):
        print(f'trying to insert data for user -> {self.mpid}, script -> {self.insertion_info}')
        try:
            engine = DBHandler().get_engine()
            metadata = MetaData()
            table_insert_to = Table(self.insertion_table, metadata, autoload_with=engine)

            print(table_insert_to)
            self.insertion_object.to_sql(self.insertion_table, engine, if_exists=self.type_sql, index=False)
            # todo !check for constrains and if not use to_sql method!
            with engine.begin() as conn:
                insert_me = insert(table_insert_to).values(self.insertion_object.to_dict(orient='records'))
                print('---')
                insert_me = insert_me.on_conflict_do_nothing()

                try:
                    # todo !query not note!
                    conn.execute(self.query)
                    conn.execute(insert_me)
                except Exception as e:
                    print(e)
                    return e

            #
            # self.insertion_object.to_sql(self.insertion_table, engine, if_exists=self.type_sql, index=False)

        except Exception as e:
            print(f"got an exception:{e}")
            return e

        print("database insertion completed successfully")
        if self.sh_flag:
            print("starting google spreadsheet insetrtion")
            # google spreadsheet insertion

            # spreadsheet_object = SPHandler()
            # worksheet = spreadsheet_object.return_worksheet_object(self.spreadsheet_data['id'], self.insertion_place)
            # values_to_spreadsheet = self.insertion_object.values.tolist()
            # values_len = len(values_to_spreadsheet)
            # cycle_counter = 0
            #
            # while cycle_counter < 3:
            #     cycle_counter += 1
            #     try:
            #         worksheet.batch_clear(["A2:N20000"])
            #         worksheet.update(f'B2:N{values_len + 1}', values_to_spreadsheet)
            #         cycle_counter = 3
            #     except Exception as e:
            #         print(e)
            #         time.sleep(10)
            #
            # print(f"completed successfully for user with mpid = {self.mpid}")
        else:
            print("no spreadsheet insertion flag found. everything completed successfully.")
            return True









