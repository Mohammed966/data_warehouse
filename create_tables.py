from db import connect
from sql_queries import create_table_queries, drop_table_queries
import psycopg2


def drop_tables(cur, conn):
    """
    Drop any existing tables from sparkifydb.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    print("Tables Dropped.")

def create_tables(cur, conn):
    """Create new tables [songplays, users, artists, songs, time] to sparkifydb."""

    print("Creating tables..")
    for query in create_table_queries:
        
        cur.execute(query)
        conn.commit()
            
    print("Tables Created.")

def main(verbose=False):
    """Connect to AWS Redshift, create new DB (sparkifydb),
        drop any existing tables, create new tables. Close DB connection.
    """
    cur, conn = connect() 
    if verbose:
        print("Dropping tables...")
    drop_tables(cur, conn)
    if verbose:
        print("Creating tables...")
    create_tables(cur, conn)
    if verbose:
        print("Done.")
    conn.close()


if __name__ == "__main__":
    main()
