import pandas as pd
import time
import create_sql as cs
from datetime import datetime
import deleting_values as dv
conn = cs.pg2()





cur = conn.cursor()

cur.execute('''select distinct coin
from coinbase_ledger2
where coin is not null
and coin not in ('USDC', 'USD')
union
select distinct symbol
from coinbasepro_ledger2
where 1=1
and symbol not in ('USDC', 'USD')
union
select distinct symbol
from binance_ledger
where symbol is not null
union
select distinct coin
from wallet''')




fin_df = pd.DataFrame.from_dict({'symbol':[],
                                'price':[],
                                'qty':[],
                                'quoteQty':[],
                                'time':[],
                                'isBuyer':[],
                                'rolling_qty':[],
                                'rolling_basis':[],
                                'avg_price':[],
                                'realized_gain_total':[]
})


row = cur.fetchall()
for r in row:
    print(r[0])
    coin=r[0]





#update to stored procedure asap
    sql = '''

    select symbol, price, qty, "quoteQty",time,"isBuyer"
    from binance_ledger
    where 1=1
    and symbol = '{}'
    union
    select coin, unit_price, amt, basis, created_at, resource
    from coinbase_ledger2
    where 1=1
    and coin = '{}'
    union
    select symbol, price,size, total,created_at, side
	from coinbasepro_ledger2
	where 1=1
	and symbol = '{}'


    union
        select d.coin, d.price,sum(d.amt) amt, d.basis, d.date_added,type1
	from
	(
	select distinct wallet.coin,
     0 price ,
   case when wallet.coin = 'BNB' then wallet.amt::NUMERIC + 0 else wallet.amt::NUMERIC end amt
    ,0 basis
    ,wallet.date_added,
    'wallet' type1
    	from wallet
    	join (
    	select max(date_added) date, coin, address
    	from wallet
    		group by  coin, address
    	) b on b.coin = wallet.coin and b.date = wallet.date_added
    	where wallet.coin = '{}'

		) d

		group by d.coin, d.price, d.basis, d.date_added,type1
    order by 5
     '''.format(coin,coin,coin,coin)



    df = pd.read_sql_query(sql,conn)

    print(df['time'] )
    #df.to_excel('../test.xlsx')
    df['time'] = df['time']#.dt.tz_localize(None)
    df['rolling_qty'] =0.00000000
    df['rolling_basis'] =0.00000000
    df['avg_price'] =0.00000000
    df['realized_gain_total'] = 0.00000000
    rolling_basis = 0.00000000
    rolling_qty = 0.00000000

    realized_gain = 0.00000000
    avg_price = 0.00000000
    realized_gain_total = 0.00000000

    for index, row in df.iterrows():
        #print('Doing shit')
        if  row['isBuyer'].upper()  == 'BUY':
            #print(row['isBuyer'])
            #print('buy',index)
            rolling_basis = rolling_basis + row['quoteQty']
            rolling_qty = rolling_qty + row['qty']
            #precent = row['qty']/qty
            try:
                avg_price = rolling_basis / rolling_qty
            except ZeroDivisionError:
                avg_price = 0

            df.at[index,'rolling_qty'] = rolling_qty
            df.at[index,'rolling_basis'] = rolling_basis
            df.at[index,'avg_price'] = avg_price
            df.at[index,'realized_gain_total'] = realized_gain_total
            #avg_price = rolling_quoteQty / qty

        if  row['isBuyer'].upper() == 'WALLET':
            rolling_qty = rolling_qty + row['qty']
            try:
                avg_price = rolling_basis / rolling_qty
            except ZeroDivisionError:
                avg_price = 0

            df.at[index,'rolling_qty'] = rolling_qty
            df.at[index,'rolling_basis'] = rolling_basis
            df.at[index,'avg_price'] = avg_price
            df.at[index,'realized_gain_total'] = realized_gain_total

        if  row['isBuyer'].upper() == 'SELL':
            #print('sell')
            #realized_gain_total = realized_gain_total + rolling_basis + row['quoteQty']
            realized_gain_total = realized_gain_total + (row['qty'] * avg_price)*-1  + row['quoteQty']
            rolling_qty = rolling_qty + row['qty']
            rolling_basis = rolling_qty * avg_price
            try:
                avg_price = rolling_basis / rolling_qty
            except ZeroDivisionError:
                avg_price = 0

            print(avg_price,rolling_basis,rolling_qty)
            df.at[index,'rolling_qty'] = rolling_qty
            df.at[index,'rolling_basis'] = rolling_basis
            df.at[index,'avg_price'] = avg_price
            df.at[index,'realized_gain_total'] = realized_gain_total
        if (row['isBuyer'].upper() == 'SEND') or (row['isBuyer'].upper() == 'pro_withdrawal'.upper())or (row['isBuyer'].upper() == 'exchange_deposit'.upper())or (row['isBuyer'].upper() == 'pro_deposit'.upper()):
            rolling_qty = rolling_qty + row['qty']
            try:
                avg_price = rolling_basis / rolling_qty
            except ZeroDivisionError:
                avg_price = 0
            df.at[index,'rolling_qty'] = rolling_qty
            df.at[index,'rolling_basis'] = rolling_basis
            df.at[index,'avg_price'] = avg_price
            df.at[index,'realized_gain_total'] = realized_gain_total

        if  row['isBuyer'].upper()  == 'TRADE':
            #print(row['isBuyer'])
            if row['quoteQty'] <= 0:
                realized_gain_total = realized_gain_total + (row['qty'] * avg_price)*-1  + row['quoteQty']
                rolling_qty = rolling_qty + row['qty']
                rolling_basis = rolling_basis + row['quoteQty']
            else:
                rolling_basis = rolling_basis + row['quoteQty']
                rolling_qty = rolling_qty + row['qty']
            #precent = row['qty']/qty
            try:
                avg_price = rolling_basis / rolling_qty
            except ZeroDivisionError:
                avg_price = 0

            df.at[index,'rolling_qty'] = rolling_qty
            df.at[index,'rolling_basis'] = rolling_basis
            df.at[index,'avg_price'] = avg_price
            df.at[index,'realized_gain_total'] = realized_gain_total

        if  (row['isBuyer'].upper()  == 'DIV') or (row['isBuyer'].upper()  == 'INFLATION_REWARD') :
            #print(row['isBuyer'])
            #print('buy',index)
            rolling_basis = rolling_basis
            rolling_qty = rolling_qty + row['qty']
            #precent = row['qty']/qty
            try:
                avg_price = rolling_basis / rolling_qty
            except ZeroDivisionError:
                avg_price = 0

            df.at[index,'rolling_qty'] = rolling_qty
            df.at[index,'rolling_basis'] = rolling_basis
            df.at[index,'avg_price'] = avg_price
            df.at[index,'realized_gain_total'] = realized_gain_total
        df.at[index,'index'] = index
    fin_df = fin_df.append(df)
    #print('qty',rolling_qty,'rolling_basis',rolling_basis,'avg_price',avg_price,'realized_gain_total',realized_gain_total)

df_max_date = fin_df.groupby('symbol')['index'].max()

df_max_date = df_max_date.to_frame()

df_max_date = df_max_date.merge(fin_df, how = 'inner', on =['index','symbol'])

df_max_date['asofdate'] = datetime.now()

fin_df = fin_df.drop('index',axis=1)
df_max_date = df_max_date.drop(['index','time'],axis=1)

print(df_max_date)



cur.close()
conn.close()

engine = cs.sql_alc()
fin_df.to_sql('avg_prices_ledger',con=engine, if_exists = 'replace', index = True)
df_max_date.to_sql('crypto_portfolio_historical',con=engine, if_exists = 'append', index = False)
#fin_df.to_excel('../testing3.xlsx')

date_ran = pd.DataFrame.from_dict({'asof':[datetime.now()]})
date_ran.to_sql('as_of_date',con=engine, if_exists = 'replace', index = True)

engine.dispose()

#dv.deleteing_historical()

print('Avg cost done')
