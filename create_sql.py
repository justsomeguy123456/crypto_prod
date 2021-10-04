



def sql_alc():
    from sqlalchemy import create_engine
    with open('../postgres_local.txt', 'r') as fp:
        lines = fp.readlines()



    db_name = lines[2].strip()
    db_uname = lines[4].strip()
    db_pw = lines[6].strip()
    db_ip = str(lines[8].strip())

    engine_string = ('postgresql://{}:{}@{}:5432/{}').format(db_uname,db_pw,db_ip,db_name)
    #print(engine_string)
    engine = create_engine(engine_string)

    return engine


def pg2():
    import psycopg2 as pyd
    with open('../postgres_local.txt', 'r') as fp:
        lines = fp.readlines()



    db_name = lines[2].strip()
    db_uname = lines[4].strip()
    db_pw = lines[6].strip()
    db_ip = str(lines[8].strip())


    conn = pyd.connect(host = db_ip, database = db_name, user = db_uname, password = db_pw )



    return conn
