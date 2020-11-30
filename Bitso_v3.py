import numpy as np
import requests
import pandas as pd 
import sqlalchemy as sql
import datetime
from pytz import timezone
import time

x =1

while True:
    print(x,'Loop has initiated...')
    x += 1
    response = requests.get('https://api.bitso.com/v3/trades/?book=btc_mxn')
    response_boolean = response.status_code == requests.codes.ok
    time.sleep(5)
    if response_boolean:
        print('Response has been succesful...')

        json_response = response.json()
        data = json_response['payload']
        df = pd.DataFrame(data)
        #print(df.head(n=2))
        print(     type(data)  )
        engine = sql.create_engine('mysql+mysqlconnector://root:metallica911@localhost:3306/bitso_api') 


        df['created_at']=pd.to_datetime(df['created_at'])
        df['created_at'] = df['created_at'].dt.tz_convert('America/Mexico_City')

        int_conv = df.amount.astype(np.float64)
        int_conv_two = df.price.astype(np.float64)
        pesos= int_conv * int_conv_two
        df['pesos'] = pesos 
        intial_q = """INSERT INTO bitso_trades
(book, created_at,amount,maker_side,price,tid,pesos)
VALUES 
"""

        values_q = ",".join(["""('{}','{}','{}','{}','{}','{}','{}')""".format(
        row.book,
        row.created_at,
        row.amount,
        row.maker_side,
        row.price,
        row.tid,
        row.pesos) for idx, row in df.iterrows()])

        end_q = """ ON DUPLICATE KEY UPDATE
    book = values(book),
    created_at = values(created_at),
    amount = values(amount),
    maker_side = values(maker_side),
    tid = values(tid),
    pesos = values(pesos); """

        query = intial_q + values_q + end_q

        engine.execute(query)



        time.sleep(35)
    else:
        print('Response has failed')
        #print('Updating database at {}'.format(datetime.datetime.today()))
        time.sleep(30)
