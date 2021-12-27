def sql_alc():
    from sqlalchemy import create_engine
    import os
    import json as j
    from dotenv import load_dotenv

    load_dotenv()
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

    # db_name = lines[2].strip()
    # db_uname = lines[4].strip()
    # db_pw = lines[6].strip()
    # db_ip = str(lines[10].strip())

    engine_string = ("postgresql://{}:{}@{}:5432/{}").format(
        db_uname, db_pw, db_ip, db_name
    )
    # print(engine_string)
    engine = create_engine(engine_string)

    return engine


def pg2():
    import psycopg2 as pyd
    import os
    import json as j
    from dotenv import load_dotenv

    load_dotenv()
    db_cred = j.loads(os.getenv("DB_CRED"))


    local = os.getenv("LOCAL")

    db_name = db_cred["db"]
    db_uname = db_cred["u"]
    db_pw = db_cred["pw"]

    if local == "Y":
        db_ip = db_cred["db_ip_local"]
    if local == "N":
        db_ip = db_cred["db_ip_ext"]

    conn = pyd.connect(host=db_ip, database=db_name, user=db_uname, password=db_pw)

    return conn
