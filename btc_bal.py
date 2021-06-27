import blockcypher
import pandas as pd
import create_sql as cs
from datetime import datetime

with open('../btc_acct.txt','r') as fp:
    lines = fp.readlines()




btc_dict = {'coin':[],
            'amt':[],
            'address':[]
            }




for l in lines:
    l = l.strip()

    bal = blockcypher.get_total_balance(l)
    bal = blockcypher.from_base_unit(bal, 'btc')

    btc_dict['coin'].append('BTC')
    btc_dict['amt'].append(bal)
    btc_dict['address'].append(l)

btc_df = pd.DataFrame.from_dict(btc_dict)

btc_df['date_added'] = datetime.now()


engine = cs.sql_alc()



btc_df.to_sql('wallet',con=engine, if_exists = 'append', index = False)

engine.dispose()
