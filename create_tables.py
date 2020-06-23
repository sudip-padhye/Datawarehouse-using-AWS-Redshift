import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    This function drops tables present in both Staging area and Star Schema.
    Input parameter:
    cur  - A cursor to the database
    conn - connection object of the database
    '''
    print('Dropping tables if exists...')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('Tables dropped successfully (if existed)...')


def create_tables(cur, conn):
    '''
    This function creates tables in both Staging area and Star Schema.
    Input parameter:
    cur  - A cursor to the database
    conn - connection object of the database
    '''
    print('Creating tables if not existed...')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('Tables created (if not existed)...')


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