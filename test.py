from coinbase.wallet.client import Client
import pandas as pd
import time
import create_sql as cs
import pprint as pp
from datetime import datetime
import os
import json as j
from dotenv import load_dotenv

load_dotenv()

creds = j.loads(os.getenv("CB_API"))


api_key = creds["key"].strip()
api_secret = creds["sec"].strip()



client = Client(api_key, api_secret)

# with open('../key.txt', 'r') as fp:
#    lines = fp.readlines()
# api_key = lines[7].strip()
# api_secret = lines[8].strip()


client = Client(api_key, api_secret)

tran = client.get_transactions('AMP', limit=100)

pp.pprint(tran)
