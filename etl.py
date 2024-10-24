import configparser
import psycopg2
import boto3
# from sql_queries import copy_table_queries, insert_table_queries
from sql_queries import copy_table_queries
from datetime import datetime


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('loading stage tables query %s'%query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # start_time = datetime.fromtimestamp(1541121934796/1000.0)
    # print("start time %s"%start_time)
    print("starting loading staging tables")
    load_staging_tables(cur, conn)
    print("loading staging tables is done")
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()