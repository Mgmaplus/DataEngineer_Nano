import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    Keyword arguments:
    cur -- active cursor from active psycopg2 connection.
    conn -- existing connection to the dwh database.
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    
    Keyword arguments:
    cur -- active cursor from active psycopg2 connection.
    conn -- existing connection to the dwh database.
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    - Establishes connection with the dwh database in our Redshift cluster.
    
    - Drops and Creates all tables.
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Step 1. Dropping Tables if exist.')
    drop_tables(cur, conn)
    print('Step 2. Creating Tables on Redshift cluster.')
    create_tables(cur, conn)
    print('Tables have been created succesfully.')

    conn.close()


if __name__ == "__main__":
    main()