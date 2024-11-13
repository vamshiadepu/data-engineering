import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all the staging and star schema tables if they exist"""
    for query in drop_table_queries:
        #print('drop table query %s'%query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates the staging and star schema tables"""
    for query in create_table_queries:
        #print('create table query %s'%query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Drop Tables Started.')
    drop_tables(cur, conn)
    print('Drop Tables Completed.')
    print('Create Tables Started')
    create_tables(cur, conn)
    print('Create Tables Completed.')

    conn.close()


if __name__ == "__main__":
    main()
