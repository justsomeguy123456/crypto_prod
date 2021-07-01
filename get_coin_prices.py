


import create_sql as cs

import json as j
import requests
import pandas as pd
from datetime import datetime
import getting_owned_coin_list as pwn

with open('../key.txt', 'r') as fp:
    lines = fp.readlines()


#api_key = lines[1].strip()
#api_secret = lines[2].strip()


owned_coin_list = pwn.getting_coin_list()

#conn = cs.pg2()
#cur = conn.cursor()


#cur.execute('''select distinct symbol from public.binance_ledger
#where symbol is not null
#union
#select distinct coin from public.coinbase_ledger''')

#owned_coin_list = []

#rows = cur.fetchall()
#for r in rows:
    #print(r[0])
#    owned_coin_list.append(r[0])

#print(owned_coin_list)


#cur.close()
#conn.close()


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


#remove_list = ['binance-peg-cardano','binance-peg-bitcoin-cash',"compound-governance-token","binance-peg-dogecoin","golden-ratio-token","binance-peg-litecoin","binance-peg-filecoin", "hymnode"]

#coin_df = coin_df.drop(remove_list)

engine = cs.sql_alc()

#print(coin_df)

coin_df.to_sql('prices',con=engine, if_exists = 'append', index = True)

engine.dispose()
