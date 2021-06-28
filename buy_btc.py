from binance.client import Client
import math
with open('../key.txt', 'r') as fp:
    lines = fp.readlines()

coin = 'BTCUSD'
dollar_amt = 22

api_key = lines[1].strip()
api_secret = lines[2].strip()

client = Client(api_key, api_secret, tld='us')


info = client.get_symbol_info(coin)


avg_price = tickers = client.get_ticker(symbol=coin)




for f in info['filters']:
    #print(f)
    if f['filterType'] == 'LOT_SIZE':
        stepsize = float((f['stepSize']))

pre_quant = int(round(-math.log(stepsize, 10), 0))
print(pre_quant)
print(avg_price['weightedAvgPrice'])
#this does something important but I don't know how it works yet
q = round(dollar_amt/float(avg_price['weightedAvgPrice']),pre_quant)
print(q)


order = client.order_market_buy(
    symbol=coin,
   quantity=q)

print('just burned {} dollhairs'.format(str(dollar_amt)))
