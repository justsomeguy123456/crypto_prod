


import create_sql as cs
import deleting_values as dv
import json as j
import requests
import pandas as pd
from datetime import datetime
import getting_owned_coin_list as pwn

with open('../key.txt', 'r') as fp:
    lines = fp.readlines()



owned_coin_list = pwn.getting_coin_list()


data = requests.get('https://api.coingecko.com/api/v3/coins/list?include_platform=true')

data = j.loads(data.text)

coin_dict = {'id':[],
'sym':[],
'link':[],
'price':[],
'name':[]

}

for d in data:
    #print(d)
    #print('*****************')
    for o in owned_coin_list:

        if o.lower() == d['name'].lower():
            #print(d)
            coin_dict['id'].append(d['id'])
            coin_dict['sym'].append(d['symbol'])
            coin_dict['link'].append('https://api.coingecko.com/api/v3/simple/price?ids={}&vs_currencies=usd'.format(d['id']))
            price = requests.get('https://api.coingecko.com/api/v3/simple/price?ids={}&vs_currencies=usd'.format(d['id']))
            price = j.loads(price.text)
            coin_dict['price'].append(price[d['id']]['usd'])
            coin_dict['name'].append(d['name'])
            #print(price)
            print('**********')


coin_df = pd.DataFrame.from_dict(coin_dict)
coin_df['date_time'] = datetime.now()
coin_df = coin_df.set_index("id")



engine = cs.sql_alc()

#print(coin_df)

coin_df.to_sql('prices',con=engine, if_exists = 'append', index = True)

engine.dispose()

dv.deleteing_prices()
