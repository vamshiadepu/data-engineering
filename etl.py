import configparser
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries
from sql_queries import copy_table_queries
from datetime import datetime


def load_staging_tables(cur, conn):
    """Loads staing tables using data stored in AWS S3"""
    for query in copy_table_queries:
        #print('loading stage tables query %s'%query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Inserts data in start schema tables"""
    for query in insert_table_queries:
        #print('insert into tables - query %s'%query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("loading staging tables started")
    load_staging_tables(cur, conn)
    print("loading staging tables completed")
    print("insert into tables started")
    insert_tables(cur, conn)
    print('insert into tables completed')

    print('Running Query to get songs played in November 2018')
    query = ("""
                    select distinct ss.title, ss.duration 
from 
songplays sp, 
time tm,
songs  ss 
where sp.start_time = tm.start_time
and sp.song_id = ss.song_id
and tm.year = '2018'
and tm.month = 11;
""")
    
    cur.execute(query)
    result = cur.fetchall()
    print("Result is:")
    for x in result:
        print(x)

    conn.close()


if __name__ == "__main__":
    main()
