import json as j
import os
from dotenv import load_dotenv

load_dotenv()
eth_adds = j.loads(os.getenv("ETH_ADDR_LIST"))
db_cred = j.loads(os.getenv("DB_CRED"))
for e in eth_adds:
    print(e)

u = db_cred['u']
print(u)

local = os.getenv("LOCAL")

if local == 'Y':
    print(1)
