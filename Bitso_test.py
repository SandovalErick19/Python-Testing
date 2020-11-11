import requests
import time
import pandas as pd

x =1

while True:
    print(x,'while loop initiated')
    x += 1
    response = requests.get('https://api.bitso.com/v3/trades/?book=btc_mxn')
    response_boolean = response.status_code == requests.codes.ok
    #response_boolean = False
    time.sleep(2)
    if response_boolean:
        print('Response has been succesful')

        json_response = response.json()
        data = json_response['payload']
        df = pd.DataFrame(data)
        print(df)
        time.sleep(30)
    else:
        print('Response has failed')
        #print('Updating database at {}'.format(datetime.datetime.today()))
        time.sleep(5)


    