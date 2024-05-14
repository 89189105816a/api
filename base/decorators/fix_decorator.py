
import pandas as pd
from datetime import datetime, timedelta
import time


def limit_fix_decorator(interval, base_distance):
    def inner(func):

        def wrapper(*args, **kwargs):
            init_distance = base_distance
            func_date_arg = args[0]
            dateto = datetime.date(datetime.today())

            print('---fetch overuse decoration starting---')
            print(f'---preparing to fetch initial dataframe with datefrom set to {func_date_arg}')
            current_dataframe = func(func_date_arg)

            if isinstance(current_dataframe, int):
                return current_dataframe

            while init_distance - interval > 0:
                print('sleeping for 5 secs')
                time.sleep(5)
                init_distance = init_distance - interval
                func_date_arg = dateto - timedelta(init_distance)
                print(f'---Continuing to change datefrom variable in steps by {interval}---')
                print(f'---prepearing to fetch dataframe with datefrom set to {func_date_arg}---')
                new_dataframe = func(func_date_arg)
                current_dataframe = pd.concat([current_dataframe, new_dataframe])
                current_dataframe.drop_duplicates()
                print('---')
            print(f'---fetching last dataframe with distance remaining {init_distance}---')
            func_date_arg = dateto - timedelta(init_distance)
            current_dataframe = pd.concat([current_dataframe, func(func_date_arg)])
            current_dataframe.drop_duplicates()

            return current_dataframe

        return wrapper
    return inner




