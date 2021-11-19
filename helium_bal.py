import requests
import pandas as pd
import json as j
import create_sql as cs
from datetime import datetime
import deleting_values as dv
helium_dict = {
'coin':[],
'amt':[],
'address':[]


}

with open('../hnt_wallets.txt', 'r') as fp:
    lines = fp.readlines()

for l in lines:


    url = "https://api.helium.io/v1/accounts/{}".format(l)

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    data = requests.get(url,headers =headers)
    data = j.loads(data.text)

    #print(type(data))
#print(response.links)
#print(response.url)
#print(response.headers)
    #print(data)

    helium_dict['coin'].append('HNT')
    helium_dict['amt'].append(float(data['data']['balance'])*float(0.00000001))
    helium_dict['address'].append(l)



    #print(helium_dict)

helium_df = pd.DataFrame.from_dict(helium_dict)

helium_df['date_added'] = datetime.now()

engine = cs.sql_alc()



helium_df.to_sql('wallet',con=engine, if_exists = 'append', index = False)

engine.dispose()


dv.deleting_wallet_vals('HNT')
