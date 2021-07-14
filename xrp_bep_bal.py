import requests as r
import create_sql as cs
import pandas as pd
from datetime import datetime
with open('../xrp_bep_acct.txt', 'r') as fp:
    lines = fp.readlines()



xrp_bep_dict = {'coin':[],
            'amt':[],
            'address':[]}

for l in lines:
    l = l.strip()
    data = r.get('https://explorer.binance.org/address/{}'.format(l))

    data = data.text

    a = data.split('XRP-BF2')

    b = a[4].split('"totalBalance":"')
    c= b[1].split('","')


    xrp_bep_dict['coin'].append( 'XRP')
    xrp_bep_dict['amt'].append(float(c[0]))
    xrp_bep_dict['address'].append(l.strip())



xrp_bep_df = pd.DataFrame.from_dict(xrp_bep_dict)
xrp_bep_df['date_added'] = datetime.now()


engine = cs.sql_alc()



xrp_bep_df.to_sql('wallet',con=engine, if_exists = 'append', index = False)

engine.dispose()


    #print(c[0])
