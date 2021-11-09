import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Loading of log datset and song dataset to current loaded database.
    
    Keyword arguments:
    cur -- active cursor from active psycopg2 connection.
    conn -- existing connection to the dwh database.
    
    """
    # Uses the TCP port opened for our Redshift cluster to execute ec2 commands that will copy from s3 to the list of staging tables.
    for query in copy_table_queries:
        cur.execute(query)    
        conn.commit()


def insert_tables(cur, conn):
    """ Transforming and Loading of staging tables to analytical tables on Redshift to execute queries on a star based schema database.
    
    Keyword arguments:
    cur -- active cursor from active psycopg2 connection.
    conn -- existing connection to the dwh database.
    
    """
    # Uses Redshift sql cqueries to insert into analytical tables prepared for executing end user queries.
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Step 1. Loading Staging Tables into Redshift from S3 files.')
    load_staging_tables(cur, conn)
    print('Setp 2. Inserting into Analytical Tables on Redshift from staging tables.')
    insert_tables(cur, conn)
    print('Loading finished successfully.')

    conn.close()


if __name__ == "__main__":
    main()