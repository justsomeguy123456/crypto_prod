from web3 import Web3
import create_sql as cs
import pandas as pd
from datetime import datetime
import deleting_values as dv
import json
import os
from dotenv import load_dotenv

load_dotenv()
eth_adds = json.loads(os.getenv("ETH_ADDR_LIST"))

key = os.getenv("INFURA_API_KEY").strip()
# with open('../infura.txt', 'r') as fp:
#    lines = fp.readlines()


w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/{}".format(key.strip())))

eth_dict = {"coin": [], "amt": [], "address": []}


# with open('../eth_wallets.txt', 'r') as fp:
#    lines = fp.readlines()

for l in eth_adds:
    eth_dict["amt"].append(w3.fromWei(w3.eth.get_balance(l.strip()), "ether"))
    eth_dict["coin"].append("ETH")
    eth_dict["address"].append(l.strip())
    print(w3.fromWei(w3.eth.get_balance(l.strip()), "ether"))


eth_df = pd.DataFrame.from_dict(eth_dict)
eth_df["date_added"] = datetime.now()


# print(engine_string)
engine = cs.sql_alc()


eth_df.to_sql("wallet", con=engine, if_exists="append", index=False)

engine.dispose()


dv.deleting_wallet_vals("ETH")
