
import datetime
from base.tasks.helpers.BaseClasses import *
from dotenv import load_dotenv
from sqlalchemy import text
import os


load_dotenv()

tg_token = os.getenv('TELEGRAM_TOKEN')
tg_channel = os.getenv('CHANNEL_ID')


class DecoCount:
    errored_mpid_list = []
    error_codes_list = []
    reports_list = []

    def __init__(self):
        self.success_counter = 0
        self.counter = 0
        self.current_mpid = 0
        self.error_codes_list = set()

    def good_increment(self):
        self.success_counter += 1
        self.counter += 1

    def bad_increment(self):
        self.counter += 1

    def make_log(self, mpid):
        if self.success_counter == self.counter:
            pass
        else:
            DecoCount.errored_mpid_list.append(mpid)
            DecoCount.error_codes_list.append(self.error_codes_list)

    @staticmethod
    def log_export():
        if not DecoCount.errored_mpid_list:
            message = 'Обновление отчетов [sales, orders] прошло без ошибок.'
            DecoCount.errored_mpid_list = None
            DecoCount.error_codes_list = None
            status = 'OK'
        else:
            message = f'Обновление отчетов [loaded_scripts] прошло c ошибками для пользователей -->{DecoCount.errored_mpid_list}, ' \
                      f'коды ошибок --> {DecoCount.error_codes_list}.'
            status = 'Error'

        engine = DBHandler().get_engine()
        connection = engine.connect()

        db_insertion_flag = True
        try:
            with connection as connection:
                query = text(f"""
                    INSERT INTO log_table (status, date, association, errors, mpid) 
                    VALUES ('{status}', '{datetime.now()}', 'None', '{DecoCount.error_codes_list}', 
                    '{DecoCount.errored_mpid_list}' )
                """)
                connection.execute(query)
        except Exception as e:
            db_insertion_flag = False

        if db_insertion_flag:
            message = message + ' Данные лога внесены в базу.'
        else:
            message = message + f' Данные лога не удалось внести в базу --> {e}'

        print(message)

        bot = Bot(token=tg_token)
        try:
            bot.send_message(chat_id=tg_channel, text=message, parse_mode=ParseMode.HTML)
        except TelegramError as e:
            print(e)


cnt = DecoCount()


def logger_decorate(func):

    def wrapper(*args, **kwargs):
        print('---logger init---')
        fetching_result = func(*args, **kwargs)
        # если mpid изменился делаем лог, очищаем счетчики и идем дальше
        if args[0] != cnt.current_mpid:
            cnt.make_log(cnt.current_mpid)
            cnt.success_counter = 0
            cnt.counter = 0
            cnt.current_mpid = args[0]

        if fetching_result is not True:
            cnt.bad_increment()
            cnt.error_codes_list.add(fetching_result)
        else:
            cnt.good_increment()
        return fetching_result

    return wrapper
