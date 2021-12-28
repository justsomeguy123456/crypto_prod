from etherscan import Etherscan
import pandas as pd
from datetime import datetime
import json
import os
from dotenv import load_dotenv


import create_sql as cs

load_dotenv()

eth = os.getenv("ETHER_SCAN_API_KEY")
# eth= Etherscan('2DRKR4USK12DDTGBYMNHD37VHJ46RYX5PT') # key in quotation marks this key is dead

gas = eth.get_gas_oracle()

gas_dict = {
    "LastBlock": [],
    "SafeGasPrice": [],
    "ProposeGasPrice": [],
    "FastGasPrice": [],
}


gas_dict["LastBlock"].append(gas["LastBlock"])
gas_dict["SafeGasPrice"].append(int(gas["SafeGasPrice"]))
gas_dict["ProposeGasPrice"].append(int(gas["ProposeGasPrice"]))
gas_dict["FastGasPrice"].append(int(gas["FastGasPrice"]))


gas_df = pd.DataFrame.from_dict(gas_dict)
gas_df["date_added"] = datetime.now()
gas_df["Unit"] = "Gwei"
engine = cs.sql_alc()

gas_df.to_sql("eth_gas", con=engine, if_exists="append", index=False)

engine.dispose()

print(gas_df)
