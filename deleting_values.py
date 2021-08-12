

def deleting_wallet_vals(sym):
    import create_sql as cs

    conn = cs.pg2()

    cur = conn.cursor()



    sql = '''


    delete from wallet where cast(date_added as varchar)||address not in (

    select cmb from (

    select max(date_added) date_added, coin, address, cast(date_added as date) as "date", cast(max(date_added) as varchar)||address as cmb
    from wallet
    where coin = '{}'
    group by coin, cast(date_added as date), address

    ) a
    ) and coin = '{}'


    '''.format(sym,sym)

    cur.execute(sql)
    conn.commit()


    cur.close()
    conn.close()

def deleteing_prices():
    import create_sql as cs

    conn = cs.pg2()
    cur = conn.cursor()
    sql = '''
    delete from prices where date_time not in (


    select date_time
    from (
    select max(date_time) date_time, cast(date_time as date) as "Date"
    from prices
    group by cast(date_time as date)
    ) a


    	)
        '''
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def deleteing_historical():
    import create_sql as cs

    conn = cs.pg2()
    cur = conn.cursor()
    sql = '''
    delete from crypto_portfolio_historical where asofdate not in(

    select asofdate from(

    select max(asofdate) asofdate, cast(asofdate as date)  date
    from crypto_portfolio_historical
    group by cast(asofdate as date)
    ) a
    	)
        '''
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
