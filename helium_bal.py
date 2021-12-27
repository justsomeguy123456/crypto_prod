import requests
import pandas as pd
import json as j
import create_sql as cs
from datetime import datetime
import deleting_values as dv
import os
from dotenv import load_dotenv
load_dotenv()
helium_dict = {
'coin':[],
'amt':[],
'address':[]


}


hnt_adds = j.loads(os.getenv("HNT_ADDR_LIST"))
#with open('../hnt_wallets.txt', 'r') as fp:
#    lines = fp.readlines()

for l in hnt_adds:

    print(l)
    url = "https://api.helium.io/v1/accounts/{}".format(l.strip())

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.37'}

    data_raw = requests.get(url,headers =headers)
    data = j.loads(data_raw.text)

    print(data)
    #print(type(data))
    print(data_raw.links)
#print(response.url)
#print(response.headers)
    #print(data)
    print(float(data['data']['balance'])*float(0.00000001))
    helium_dict['coin'].append('HNT')
    helium_dict['amt'].append(float(data['data']['balance'])*float(0.00000001))
    helium_dict['address'].append(l)



    #print(helium_dict)

helium_df = pd.DataFrame.from_dict(helium_dict)

helium_df['date_added'] = datetime.now()

print(helium_df)

engine = cs.sql_alc()



helium_df.to_sql('wallet',con=engine, if_exists = 'append', index = False)

engine.dispose()


dv.deleting_wallet_vals('HNT')
