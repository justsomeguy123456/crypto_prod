import pandas as pd
import create_sql as cs

engine = cs.sql_alc()

sql = 'select * from avg_prices_ledger'

df = pd.read_sql(con = engine, sql = sql)

print(df)
