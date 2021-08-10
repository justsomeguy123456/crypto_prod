
import sys
import math
import pprint as pp

'''
args should be
coin
coinbasepro or binance
dollar amt

'''
amt = float(sys.argv[3])

if sys.argv[2].lower() == 'coinbasepro':


    import cbpro

    with open('../coinbasepro_key.txt', 'r') as fp:
        lines = fp.readlines()

    key = lines[0].strip()
    b64secret = lines[1].strip()
    passphrase = lines[2].strip()



    coin = sys.argv[1]

    product = coin +'-USD'

    auth_client = cbpro.AuthenticatedClient(key, b64secret, passphrase)
    currencies = auth_client.get_currencies()

    for c in currencies:
        if c['id']==coin:
            pp.pprint(c)
            min_size = float(c['min_size'])
            stepsize = float(c['max_precision'])
            print(stepsize)
            pre_quant = int(round(-math.log(stepsize, 10), 0))
            print(pre_quant)

    ticker = auth_client.get_product_ticker(product_id=product)
    price= float(ticker['ask'])

    qnt = round((amt/price), pre_quant)

    if qnt < min_size:
        qnt = min_size

    print(product)
    print(price,qnt)

    print('Using cbpro' )

    auth_client.place_limit_order(product_id=product,
                                  side='buy',
                                  price=str(price),
                                  size=str(qnt))


if sys.argv[2].lower() == 'binance':

    from binance.client import Client
    with open('../key.txt', 'r') as fp:
        lines = fp.readlines()

    coin = sys.argv[1].strip()+'USD'


    api_key = lines[1].strip()
    api_secret = lines[2].strip()

    client = Client(api_key, api_secret, tld='us')

    info = client.get_symbol_info(coin)
    avg_price = tickers = client.get_ticker(symbol=coin)

    for f in info['filters']:

        if f['filterType'] == 'LOT_SIZE':

            stepsize = float((f['stepSize']))

    pre_quant = int(round(-math.log(stepsize, 10), 0))


    q = round(amt/float(avg_price['weightedAvgPrice']),pre_quant)
    print(coin)
    print(avg_price['weightedAvgPrice'],q)

    print('Using binance' )
    order = client.order_market_buy(
        symbol=coin,
        quantity=q)
