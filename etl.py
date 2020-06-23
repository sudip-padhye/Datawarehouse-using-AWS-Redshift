import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function loads tables present in Staging area.
    Input parameter:
    cur  - A cursor to the database
    conn - connection object of the database
    '''
    print('Loading Staging tables...')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Staging tables loaded...')


def insert_tables(cur, conn):
    '''
    This function loads tables present in Star Schema.
    Input parameter:
    cur  - A cursor to the database
    conn - connection object of the database
    '''
    print('Inserting into Star Schema...')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('Star Schema loaded...')


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