import pandas as pd
from binance.client import Client
import json as j
from datetime import datetime, timedelta
import create_sql as cs
from dateutil.tz import tzutc
import time
import os
from dotenv import load_dotenv

load_dotenv()

creds = j.loads(os.getenv("BINANCE_API"))
# with open('../key.txt', 'r') as fp:
#    lines = fp.readlines()


# api_key = lines[1].strip()
# api_secret = lines[2].strip()

api_key = creds["key"].strip()
api_secret = creds["sec"].strip()


client = Client(api_key, api_secret, tld="us")


""" this is getting list of coins. in the future
this list will be every coin listed on binance    """

# coin = ['BTC','ETH','ADA','DOGE','LINK','ATOM','BNB','LTC','XLM','SOL','VTHO']
prices = client.get_all_tickers()
coin = []

for p in prices:
    s = p["symbol"]
    if s[-3:] == "USD":
        print(s)
        sy = s[0 : len(s) - 3]
        # print(sy)
        # print('******')
        coin.append(sy)
# this is master dict for final dataframe
trade_dict = {
    "symbol": [],
    "id": [],
    "orderId": [],
    "orderListId": [],
    "price": [],
    "qty": [],
    "quoteQty": [],
    "commission": [],
    "commissionAsset": [],
    "commission_val": [],
    "time": [],
    "isBuyer": [],
    "isMaker": [],
    "isBestMatch": [],
}


# getting trade info
for c in coin:

    trades = client.get_my_trades(symbol=c + "USD")

    for t in trades:

        trade_dict["symbol"].append(c)
        trade_dict["id"].append(t["id"])
        trade_dict["orderId"].append(t["orderId"])
        trade_dict["orderListId"].append(t["orderListId"])
        trade_dict["price"].append(float(t["price"]))
        # subtracting out qunanity where the coin was used to pay commission. ignoring bnb because that is handled later
        if t["commissionAsset"] == c and t["commissionAsset"] != "BNB":
            trade_dict["qty"].append(float(t["qty"]) - float(t["commission"]))
        else:
            trade_dict["qty"].append(float(t["qty"]))
        trade_dict["quoteQty"].append(float(t["quoteQty"]))
        trade_dict["commission"].append(float(t["commission"]))
        trade_dict["commissionAsset"].append(t["commissionAsset"])
        # getting value of commission in USD. had to make assumption that fee is .00075
        if t["commissionAsset"] == "USD":
            trade_dict["commission_val"].append(float(t["commission"]))
        else:
            trade_dict["commission_val"].append(float(t["quoteQty"]) * 0.00075)
        trade_dict["time"].append(t["time"])
        if t["isBuyer"] == 1:
            trade_dict["isBuyer"].append("Buy")
        else:
            trade_dict["isBuyer"].append("Sell")
        # trade_dict['isBuyer'].append( t['isBuyer'])
        trade_dict["isMaker"].append(t["isMaker"])
        trade_dict["isBestMatch"].append(t["isBestMatch"])
    time.sleep(0.2)
trade_df = pd.DataFrame.from_dict(trade_dict)
trade_df["commission_val"] = trade_df["commission_val"].abs()
trade_df["qty"] = pd.to_numeric(trade_df["qty"])
trade_df["quoteQty"] = pd.to_numeric(trade_df["quoteQty"])

trade_df.loc[trade_df["isBuyer"] == "Sell", "qty"] = trade_df["qty"] * -1
trade_df.loc[trade_df["isBuyer"] == "Sell", "quoteQty"] = trade_df["quoteQty"] * -1

# getting bnb commission
df1 = trade_df[trade_df["commissionAsset"] == "BNB"]
df1 = df1.copy(deep=True)
df1.dropna()


df1["symbol"] = "BNB"
df1["price"] = ((df1["quoteQty"] * 0.00075) / pd.to_numeric(df1["commission"])).abs()
df1["qty"] = pd.to_numeric(df1["commission"]) * -1.0
df1["commission"] = 0
df1["commissionAsset"] = ""
df1["commission_val"] = 0
df1["quoteQty"] = df1["qty"].abs() * df1["price"]
df1["isBuyer"] = "Sell"

trade_df = trade_df.append(df1, ignore_index=True)

# getting deposits and withdrwas
deposit_dict = {
    "symbol": [],
    "id": [],
    "orderId": [],
    "orderListId": [],
    "price": [],
    "qty": [],
    "quoteQty": [],
    "commission": [],
    "commissionAsset": [],
    "commission_val": [],
    "time": [],
    "isBuyer": [],
    "isMaker": [],
    "isBestMatch": [],
}


deposits = client.get_deposit_history()
withdraws = client.get_withdraw_history()


for d in deposits:
    deposit_dict["symbol"].append(d["coin"])
    deposit_dict["id"] = ""
    deposit_dict["orderId"] = ""
    deposit_dict["orderListId"] = ""
    deposit_dict["price"] = 0
    deposit_dict["qty"].append(float(d["amount"]))
    deposit_dict["quoteQty"] = 0
    deposit_dict["commission"].append(0)
    deposit_dict["commissionAsset"].append("")
    deposit_dict["commission_val"] = 0
    deposit_dict["time"].append(d["insertTime"])
    deposit_dict["isBuyer"] = "Send"
    deposit_dict["isMaker"] = 0
    deposit_dict["isBestMatch"] = 0

for w in withdraws:
    deposit_dict["symbol"].append(w["coin"])
    deposit_dict["id"] = ""
    deposit_dict["orderId"] = ""
    deposit_dict["orderListId"] = ""
    deposit_dict["price"] = 0
    deposit_dict["qty"].append(
        float(w["amount"]) * -1.000000000000
        + float(w["transactionFee"]) * -1.000000000000
    )
    deposit_dict["quoteQty"] = 0
    deposit_dict["commission"].append(float(w["transactionFee"]))
    deposit_dict["commissionAsset"].append(w["coin"])
    deposit_dict["commission_val"] = 0
    deposit_dict["time"].append(
        (
            datetime.timestamp(datetime.strptime(w["applyTime"], "%Y-%m-%d %H:%M:%S"))
            * 1000
        )
        - 14400000
    )
    deposit_dict["isBuyer"] = "Send"
    deposit_dict["isMaker"] = 0
    deposit_dict["isBestMatch"] = 0


d_w_df = pd.DataFrame.from_dict(deposit_dict)

trade_df = trade_df.append(d_w_df, ignore_index=True)


# getting div history
divs = client.get_asset_dividend_history()

div_dict = {
    "symbol": [],
    "id": [],
    "orderId": [],
    "orderListId": [],
    "price": [],
    "qty": [],
    "quoteQty": [],
    "commission": [],
    "commissionAsset": [],
    "commission_val": [],
    "time": [],
    "isBuyer": [],
    "isMaker": [],
    "isBestMatch": [],
}


for h in divs["rows"]:
    div_dict["symbol"].append(h["asset"])
    div_dict["id"] = ""
    div_dict["orderId"] = ""
    div_dict["orderListId"] = ""
    div_dict["price"] = 0
    div_dict["qty"].append(float(h["amount"]))
    div_dict["quoteQty"] = 0
    div_dict["commission"].append(0)
    div_dict["commissionAsset"].append("")
    div_dict["commission_val"] = 0
    div_dict["time"].append(h["divTime"])
    div_dict["isBuyer"] = "Div"
    div_dict["isMaker"] = 0
    div_dict["isBestMatch"] = 0

div_df = pd.DataFrame.from_dict(div_dict)
trade_df = trade_df.append(div_df, ignore_index=True)


trade_df["time"] = pd.to_datetime(trade_df["time"], unit="ms", utc=True)

manual_df = pd.read_excel("../binance_ledger_man1.xlsx")
manual_df["time"] = pd.to_datetime(manual_df["time"], utc=True)
trade_df = trade_df.append(manual_df)
# trade_df['time'].dt.tz_localize('UTC')


trade_df["date_added"] = datetime.now()


# trade_df.to_excel('../binance_ledger.xlsx')


conn = cs.pg2()

cur = conn.cursor()

cur.execute("select max(bl.time) date from public.binance_ledger bl")


row = cur.fetchone()
for r in row:
    date = row[0]
date = date + timedelta(seconds=1)

trade_df = trade_df[trade_df["time"] > date]

trade_df = trade_df.copy(deep=True)

cur.close()
conn.close()


print(type(date))

engine = cs.sql_alc()


trade_df.to_sql("binance_ledger", con=engine, if_exists="append", index=False)

engine.dispose()
