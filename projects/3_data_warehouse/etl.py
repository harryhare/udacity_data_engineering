import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''
    This function load staging tables from S3
    
    INPUTS: 
    * cur the cursor variable
    * conn the database connection
'''
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("done")

'''
    This function extract data from staging tables and insert into five analysis tables
    
    INPUTS: 
    * cur the cursor variable
    * conn the database connection
'''
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("done")

'''
    This function load data to tables.
'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close() 


if __name__ == "__main__":
    main()