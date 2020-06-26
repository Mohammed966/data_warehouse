from db import connect
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """Load JSON input data (log_data and song_data) from S3 and insert
        into staging_events and staging_songs tables.
    """


    print("Loading staging tables...")
    for query in copy_table_queries:
        print("Executing query '{}'...".format(query))
        cur.execute(query)
        conn.commit()
    print("Staging tables loaded ok")

    



def insert_tables(cur, conn):

    """Insert data from staging tables (staging_events and staging_songs)
        into star schema analytics tables:
        Fact table: songplays
        Dimension tables: users, songs, artists, time
    """

    
    for query in insert_table_queries:
        print("Executing query '{}'...".format(query))
        cur.execute(query)
        conn.commit()
    print("Fact and dimension tables loaded ok")

def main():

    """Connect to DB"""


    cur , conn = connect() 
    print("AWS Redshift connection established OK.")
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
