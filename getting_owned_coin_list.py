

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
