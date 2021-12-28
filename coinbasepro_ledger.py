import cbpro
import pprint as pp
import pandas as pd
from itertools import islice
import time
from datetime import datetime
import create_sql as cs
import os
import json as j
from dotenv import load_dotenv

load_dotenv()

creds = j.loads(os.getenv("CB_PRO_API"))


key = creds["key"].strip()
b64secret = creds["sec"].strip()
passphrase = creds["pass"].strip()

# with open('../coinbasepro_key.txt', 'r') as fp:
#    lines = fp.readlines()

# key = lines[0].strip()
# b64secret = lines[1].strip()
# passphrase = lines[2].strip()


auth_client = cbpro.AuthenticatedClient(key, b64secret, passphrase)
accts = auth_client.get_accounts()


fills_dict = {
    "symbol": [],
    "created_at": [],
    "fee": [],
    "liquidity": [],
    "order_id": [],
    "price": [],
    "product_id": [],
    "profile_id": [],
    "settled": [],
    "side": [],
    "size": [],
    "trade_id": [],
    "usd_volume": [],
    "user_id": [],
    "total": [],
}

# history_list = list(auth_client.get_account_history('475d055e-591d-41d3-9563-5339ee679ac3',limit=100))
for a in accts:
    history_list = list(auth_client.get_account_history(a["id"].strip(), limit=100))
    coin = a["currency"].strip()

    print(history_list)

    fills_list = list(auth_client.get_fills(product_id=coin + "-USD"))

    for f in fills_list:
        # print(f)
        try:
            fills_dict["symbol"].append(f["product_id"].split("-")[0])
            fills_dict["created_at"].append(f["created_at"])
            fills_dict["fee"].append(float(f["fee"]))
            fills_dict["liquidity"].append(f["liquidity"])
            fills_dict["order_id"].append(f["order_id"])
            fills_dict["price"].append(float(f["price"]))
            fills_dict["product_id"].append(f["product_id"])
            fills_dict["profile_id"].append(f["profile_id"])
            fills_dict["settled"].append(f["settled"])
            fills_dict["side"].append(f["side"])

            fills_dict["trade_id"].append(f["trade_id"])
            if f["side"].lower() == "buy":
                print("buy")
                print(f["size"])
                fills_dict["usd_volume"].append(float(f["usd_volume"]) * float(1.00))
                fills_dict["total"].append(
                    float(f["fee"]) + float(f["usd_volume"]) * float(1.00)
                )
                fills_dict["size"].append(float(f["size"]))
            elif f["side"].lower() == "sell":
                print("sell")
                print(f["size"])
                fills_dict["usd_volume"].append(float(f["usd_volume"]) * -1.00)
                fills_dict["total"].append(
                    ((float(f["fee"]) + float(f["usd_volume"])) * -1.00)
                )
                fills_dict["size"].append(float(f["size"]) * -1.00)
            fills_dict["user_id"].append(f["user_id"])
        except:
            print("empty")

    for h in history_list:

        if h["type"] == "transfer" and coin == "ENJ":
            fills_dict["symbol"].append(coin)
            fills_dict["created_at"].append(h["created_at"])
            fills_dict["fee"].append(0.000000)
            fills_dict["liquidity"].append("")
            fills_dict["order_id"].append(h["details"]["transfer_id"])
            fills_dict["price"].append(0.00000)
            fills_dict["product_id"].append("")
            fills_dict["profile_id"].append("")
            fills_dict["settled"].append("")
            fills_dict["side"].append("send")
            fills_dict["size"].append(float(h["amount"]))
            fills_dict["trade_id"].append("")

            fills_dict["usd_volume"].append(0.000000)
            fills_dict["total"].append(0.000000)

            fills_dict["user_id"].append("")

    time.sleep(0.5)
pp.pprint(fills_dict)
fills_df = pd.DataFrame.from_dict(fills_dict)
fills_df["date_added"] = datetime.now()
fills_df["created_at"] = pd.to_datetime(fills_df["created_at"])
# fills_df.to_excel('../test2.xlsx')


conn = cs.pg2()

cur = conn.cursor()
try:
    cur.execute("select max(cl.created_at) date from public.coinbasepro_ledger cl")

    row = cur.fetchone()
    for r in row:
        date = row[0]
    fills_df = fills_df[fills_df["created_at"] > date]
    fills_df = fills_df.copy(deep=True)
except:
    print("no table")

cur.close()
conn.close()


engine = cs.sql_alc()


fills_df.to_sql("coinbasepro_ledger", con=engine, if_exists="append", index=False)
engine.dispose()


# pp.pprint(history)
