from binance_chain.http import HttpApiClient
from binance_chain.constants import KlineInterval
from binance_chain.environment import BinanceEnvironment
import create_sql as cs
import deleting_values as dv
import pandas as pd
from datetime import datetime
import pprint as pp
import json as j
import os
from dotenv import load_dotenv

load_dotenv()
bnb_adds = j.loads(os.getenv("BNB_ADDR_LIST"))

#with open('../bnb_acct.txt', 'r') as fp:
#    lines = fp.readlines()



client = HttpApiClient()


bnb_dict = {'coin':[],
            'amt':[],
            'address':[]



}

for l in bnb_adds:
    l = l.strip()
    print(l)
    account = client.get_account(l)
    transactions = client.get_transactions(address=l)

    #pp.pprint(account['balances'])
    for x in account['balances']:
        #print(x)



        #print(transactions)
        if '-BF2' in x['symbol'] :
            bnb_dict['coin'].append(x['symbol'].strip('-BF2') )
            sym = (x['symbol'].strip('-BF2') )
        elif '-BD1' in x['symbol'] :
            #bnb_dict['coin'].append(x['symbol'].strip('-BD1') )
            bnb_dict['coin'].append('BUSD')
            sym = 'BUSD'#(x['symbol'].strip('-BD1') )
            print(x['symbol'])
        else:
            bnb_dict['coin'].append(x['symbol'] )
            sym = (x['symbol'] )
        bnb_dict['amt'].append(x['free'])
        bnb_dict['address'].append(l)


print(bnb_dict)

bnb_df = pd.DataFrame.from_dict(bnb_dict)
bnb_df = bnb_df[bnb_df['coin'] == 'BNB']
bnb_df['date_added'] = datetime.now()
print(bnb_df)

engine = cs.sql_alc()



bnb_df.to_sql('wallet',con=engine, if_exists = 'append', index = False)




engine.dispose()




#for c in bnb_dict['coin']:
#    print(c)
dv.deleting_wallet_vals('BNB')
