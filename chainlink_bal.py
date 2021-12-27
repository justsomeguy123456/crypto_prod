import create_sql as cs
import pandas as pd
from datetime import datetime
import deleting_values as dv
import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()
eth_adds = json.loads(os.getenv("ETH_ADDR_LIST"))
link_dict = {"coin": [], "amt": [], "address": []}

key = os.getenv("ETHER_SCAN_API_KEY").strip()
#with open("../etherscan.txt", "r") as fp:
#    lines = fp.readlines()

#key = lines[0].strip()

contract = "0x03fF0ff224f904be3118461335064bB48Df47938"

#with open('../eth_wallets.txt', 'r') as fp:
#    lines = fp.readlines()


for l in eth_adds:
    url = "https://api.etherscan.io/api?module=account&action=tokenbalance&contractaddress={}&address={}&tag=latest&apikey={}".format(
        contract, l.strip(), key
    )
    print(url)

    data = requests.get(url).json()

    print(data)
    if float(data["result"]) != 0.0:
        amt = float(data["result"]) / 1000000000000000000.0
        link_dict["amt"].append(amt)
        link_dict["coin"].append("LINK")
        link_dict["address"].append(l.strip())


link_df = pd.DataFrame.from_dict(link_dict)
link_df["date_added"] = datetime.now()

print(link_df)

# print(engine_string)
engine = cs.sql_alc()


link_df.to_sql("wallet", con=engine, if_exists="append", index=False)

engine.dispose()

dv.deleting_wallet_vals("LINK")
