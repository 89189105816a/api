
from base.tasks.helpers.BaseClasses import *
import pandas as pd


def nomenclature_from_tables(table):
    # коллектим строки в номенклатуру из таблиц reportdetailbyperiod_wb, orders_wb, sales_wb
    engine = DBHandler().get_engine()
    db_dataframe_report = pd.read_sql_query(f"SELECT * FROM reportdetailbyperiod_wb WHERE 'table' = '{table}'", engine)
    db_dataframe_orders = pd.read_sql_query(f"SELECT * FROM orders_daily_wb WHERE 'table' = '{table}'", engine)
    db_dataframe_sales = pd.read_sql_query(f"SELECT * FROM sales_daily_wb WHERE 'table' = '{table}'", engine)

    # так как в разных таблицах одни и те-же вещи называются по разному манипулируем датафреймами

    db_dataframe_orders = db_dataframe_orders[['lastChangeDate', 'barcode', 'nmId', 'subject', 'supplierArticle',
                                              'techSize', 'brand']]
    db_dataframe_orders.rename(columns={'lastChangeDate': 'updateAt', 'barcode': 'skus', 'nmId': 'nmID',
                                        'subject': 'object', 'supplierArticle': 'vendorCode'}, inplace=True)
    db_dataframe_orders['mediaFiles'] = 'no'
    db_dataframe_orders['colors'] = 'no'

    db_dataframe_report = db_dataframe_report[['ts_name', 'date_load', 'sa_name', 'brand_name', 'subject_name',
                                               'nm_id', 'barcode']]
    db_dataframe_report.rename(columns={'ts_name': 'techSize', 'date_load': 'updateAt', 'sa_name': 'vendorCode',
                                        'brand_name': 'brand', 'subject_name': 'object', 'nm_id': 'nmID',
                                        'barcode': 'skus'}, inplace=True)
    db_dataframe_report['mediaFiles'] = 'no'
    db_dataframe_report['colors'] = 'no'

    db_dataframe_sales = db_dataframe_sales[['techSize', 'lastChangeDate', 'supplierArticle', 'brand', 'subject',
                                             'nmId', 'barcode']]
    db_dataframe_sales.rename(columns={'lastChangeDate': 'updateAt', 'supplierArticle': 'vendorCode',
                                       'subject': 'object', 'nmId': 'nmID', 'barcode': 'skus'}, inplace=True)
    db_dataframe_sales['mediaFiles'] = 'no'
    db_dataframe_sales['colors'] = 'no'

    return {'report': db_dataframe_report, 'orders': db_dataframe_orders, 'sales': db_dataframe_sales}

