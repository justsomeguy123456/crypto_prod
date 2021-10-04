from coinbase.wallet.client import Client
import pandas as pd
import time
import create_sql as cs

from datetime import datetime


with open('../key.txt', 'r') as fp:
    lines = fp.readlines()
api_key = lines[7].strip()
api_secret = lines[8].strip()






client = Client(api_key, api_secret)
acct = client.get_accounts(limit = 100)

acct_dict = {'coin':[],
            'dollar_amt':[],
            'amt':[],
            'id':[]
}
for w in acct.data:

    acct_dict['coin'].append(str(w['balance']['currency']))
    acct_dict['dollar_amt'].append(str(w['native_balance']['amount']))
    acct_dict['amt'].append(str(w['balance']['amount']))
    acct_dict['id'].append(str(w['id']))



tran_dict = {'amt':[],
           'coin':[],
           'committed':[],
           'created_at':[],
           'fee_amt':[],
           'fee_currency':[],
           'resource':[],
           'status':[],
           'basis':[],
           'basis_currency':[],
           'unit_price':[],
           'unit_price_currency':[],

}

for v in acct_dict['id']:


    id = v

    buys = client.get_buys(id,limit = 100)

    sell = client.get_sells(id,limit = 100)
    tran = client.get_transactions(id,limit=100)


    for s in sell.data:

        tran_dict['amt'].append(float(s['amount']['amount'])*-1.000000000000000)
        tran_dict['coin'].append(s['amount']['currency'])
        tran_dict['committed'].append(s['committed'])
        tran_dict['created_at'].append(s['created_at'])
        tran_dict['fee_amt'].append(float(s['fees'][0]['amount']['amount']))
        tran_dict['fee_currency'].append(s['fees'][0]['amount']['currency'])
        tran_dict['resource'].append(s['resource'])
        tran_dict['status'].append(s['status'])
        tran_dict['basis'].append(float(s['subtotal']['amount']))
        tran_dict['basis_currency'].append(s['subtotal']['currency'])
        tran_dict['unit_price'].append(float(s['unit_price']['amount']))
        tran_dict['unit_price_currency'].append(s['unit_price']['currency'])


    for b in buys.data:
        tran_dict['amt'].append(float(b['amount']['amount']))
        tran_dict['coin'].append(b['amount']['currency'])
        tran_dict['committed'].append(b['committed'])
        tran_dict['created_at'].append(b['created_at'])
        tran_dict['fee_amt'].append(float(b['fees'][0]['amount']['amount']))
        tran_dict['fee_currency'].append(b['fees'][0]['amount']['currency'])
        tran_dict['resource'].append(b['resource'])
        tran_dict['status'].append(b['status'])
        tran_dict['basis'].append(float(b['subtotal']['amount']))
        tran_dict['basis_currency'].append(b['subtotal']['currency'])
        tran_dict['unit_price'].append(float(b['unit_price']['amount']))
        tran_dict['unit_price_currency'].append(b['unit_price']['currency'])

    for t in tran.data:

        #print('*************************************')
        if t['type'] == 'trade' or t['type'] == 'send':
            tran_dict['amt'].append(float(t['amount']['amount']))
            tran_dict['coin'].append(t['amount']['currency'])
            tran_dict['committed'].append('TRUE')
            tran_dict['created_at'].append(t['created_at'])
            tran_dict['fee_amt'].append(float(0))
            tran_dict['fee_currency'].append(t['native_amount']['currency'])
            tran_dict['resource'].append(t['type'])
            tran_dict['status'].append(t['status'])
            if t['type'] == 'send':
                tran_dict['basis'].append(float(0))
            else:
                tran_dict['basis'].append(float(t['native_amount']['amount']))
            tran_dict['basis_currency'].append(t['native_amount']['currency'])
            tran_dict['unit_price'].append( float(t['native_amount']['amount'])/  float(t['amount']['amount']))
            tran_dict['unit_price_currency'].append(t['native_amount']['currency'])
    print('*********')
    time.sleep(.3)

ledger_df = pd.DataFrame.from_dict(tran_dict)
ledger_df.loc[ledger_df['resource'] == 'sell', 'basis'] = ledger_df['basis'] * -1.00



ledger_df['date_added'] = datetime.now()


ledger_df.to_excel('../coinbase_ledger.xlsx')

ledger_df['created_at'] = pd.to_datetime(ledger_df['created_at'])


conn = cs.pg2()

cur = conn.cursor()

cur.execute('select max(cl.created_at) date from public.coinbase_ledger cl')



row = cur.fetchone()
for r in row:
    date = row[0]

ledger_df = ledger_df[ledger_df['created_at'] > date]

ledger_df = ledger_df.copy(deep = True)

cur.close()
conn.close()







engine = cs.sql_alc()



ledger_df.to_sql('coinbase_ledger',con=engine, if_exists = 'append', index = False)
engine.dispose()
