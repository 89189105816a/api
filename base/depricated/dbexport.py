

from base.tasks.helpers.BaseClasses import *
import pandas as pd


def table_import():
    engine = DBHandler().get_engine('db4')
    with engine.connect() as connection:
        table_name = 'log_table'
        query = f'SELECT * FROM {table_name};'
        mydf = pd.read_sql(query, connection)
    return mydf


def table_export(df):
    engine = DBHandler().get_engine('db3')
    with engine.connect() as connection:
        df.to_sql('log_table', engine, index=False)


if __name__ == '__main__':

    df = table_import()
    table_export(df)
