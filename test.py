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



creds = j.loads(os.getenv("CB_PRO_API"))


api_key = creds["key"].strip()
b64secret = creds["sec"].strip()
passphrase = creds["pass"].strip()



db_cred = j.loads(os.getenv("DB_CRED"))

local = os.getenv("LOCAL")
# with open('../postgres_local.txt', 'r') as fp:
#    lines = fp.readlines()

db_name = db_cred["db"]
db_uname = db_cred["u"]
db_pw = db_cred["pw"]

if local == "Y":
    db_ip = db_cred["db_ip_local"]
if local == "N":
    db_ip = db_cred["db_ip_ext"]

print(db_name,db_uname,db_pw,db_ip)
