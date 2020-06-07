import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


'''
    This function drops tables.
    
    INPUTS: 
    * cur the cursor variable
    * conn the database connection
'''
def drop_tables(cur, conn):
    print(len(drop_table_queries))
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("done")

'''
    This function creates tables.
    
    INPUTS: 
    * cur the cursor variable
    * conn the database connection
'''
def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print("done")


'''
    This function initialize the database.
'''
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()