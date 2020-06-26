import psycopg2

from config import CONFIG


def connect():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*CONFIG['CLUSTER'].values()))
    cur = conn.cursor()
    return cur, conn
    print(conn)
