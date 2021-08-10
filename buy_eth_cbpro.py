import cbpro
import pprint as pp
import math
with open('../coinbasepro_key.txt', 'r') as fp:
    lines = fp.readlines()

key = lines[0].strip()
b64secret = lines[1].strip()
passphrase = lines[2].strip()



auth_client = cbpro.AuthenticatedClient(key, b64secret, passphrase)


ticker = auth_client.get_currencies()


amt = float(22.00)

for t in ticker:
    if t['id']=='ETH':
        pp.pprint(t)
        stepsize = float(t['min_size'])
        pre_quant = int(round(-math.log(stepsize, 10), 0))
        print(pre_quant)


#pp.pprint(ticker)
