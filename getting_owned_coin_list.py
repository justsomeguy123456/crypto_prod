

def getting_coin_list():
    import pandas as pd
    with open('../google_sheets.txt','r') as fp:
        lines = fp.readlines()

    sheet_id = lines[1].strip()

    sheet_name = 'tickers'

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"


    df = pd.read_csv(url)

    coin_list = df['Coin'].tolist()

    return(coin_list)



owned = getting_coin_list()





def getting_wallet_bals():
    import pandas as pd
    import create_sql as cs
    from datetime import datetime
    import deleting_values as dv
    with open('../google_sheets.txt','r') as fp:
        lines = fp.readlines()

    sheet_id = lines[1].strip()

    sheet_name = 'wallets'

    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"


    df = pd.read_csv(url)

    df = df[df['use'] == 'Y']

    df = df.drop(columns=['use'])
    df = df.drop(columns=['where'])
    df['date_added'] = datetime.now()
    engine = cs.sql_alc()

    df.to_sql('wallet',con=engine, if_exists = 'append', index = False)
    engine.dispose()


    clist = df['coin'].tolist()
    for c in clist:
        print(c)
        dv.deleting_wallet_vals(c)
    print(df)
