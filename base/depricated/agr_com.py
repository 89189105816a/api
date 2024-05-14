import json
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import io
import psycopg2
from psycopg2 import Error
import sys
import time
import base64
import requests
import gspread

engine = create_engine('postgresql://postgres:TrW34Uq@localhost:5432/dashboard')
conn = psycopg2.connect(
    user="postgres",
    # пароль, который указали при установке PostgreSQL
    password="TrW34Uq",
    host="localhost",
    port="5432",
    database="dashboard")
def agr_com(engine, conn):
    sql = f"SELECT * from reportdetailbyperiod_wb where supplier_oper_name = 'Частичная компенсация брака'"
    df = pd.read_sql(sql, con=engine)
    
    table = pd.DataFrame({})

    df['barcode'] = df['barcode'].values.astype('object')
    unique_realID = df['realizationreport_id'].values.tolist()
    unique_realID = list(set(unique_realID))
   
    
    for realID in unique_realID:
        temp_table = df[df['realizationreport_id'] == realID]
        print(temp_table)
        i = 0
        
        
        df_agr = temp_table.copy()
        # df_agr = df_agr.drop(['retail_amount', 'retail_price_withdisc_rub', 'ppvz_for_pay'], axis= 1 , inplace= True )
        df_agr['retail_amount'] = 0
        df_agr['retail_price_withdisc_rub'] = 0
        df_agr['ppvz_for_pay'] = 0
        df_agr['index'] = 0

        df_agr['rrd_id'] = 0
        df_agr = df_agr.drop_duplicates()
        df_agr = df_agr.reset_index(drop=True)

        while i < len(df_agr):
            shk_id = df_agr.at[i, 'shk_id']

            barcode = str(df_agr.at[i, 'barcode'])
            df_agr.loc[i, 'retail_amount'] = temp_table.loc[temp_table['shk_id'] == shk_id, 'retail_amount'].sum()
            df_agr.loc[i, 'retail_price_withdisc_rub'] = temp_table.loc[(temp_table['shk_id'] ==  shk_id) &(temp_table['barcode'] == barcode), 'retail_price_withdisc_rub'].sum()
            df_agr.loc[i, 'ppvz_for_pay'] = temp_table.loc[(temp_table['shk_id'] ==  shk_id) &(temp_table['barcode'] == barcode), 'ppvz_for_pay'].sum()
            i += 1
        table = pd.concat([table, df_agr])
        
  
    cur = conn.cursor()
    cur.execute("DELETE FROM reportdetailbyperiod_wb WHERE supplier_oper_name = 'Частичная компенсация брака'")
    conn.commit()
    # Close communication with the PostgreSQL database
    cur.close()
    conn.close()
    table.to_sql('reportdetailbyperiod_wb', engine, if_exists='append', index=False)
    
if __name__ == "__main__":
    engine = create_engine('postgresql://postgres:TrW34Uq@localhost:5432/dashboard')
    conn = psycopg2.connect(
        user="postgres",
        # пароль, который указали при установке PostgreSQL
        password="TrW34Uq",
        host="localhost",
        port="5432",
        database="dashboard")
    agr_com(engine, conn)
