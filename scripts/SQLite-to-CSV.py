import sqlite3
import pandas as pd
import zipfile
import os

def connect_to_db(db_path):
    """Connect to the SQLite database."""
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def query_all_data(conn):
    """Query all data from the database."""
    query = "SELECT * FROM obs_sukayu_daily"
    try:
        return pd.read_sql_query(query, conn)
    except pd.io.sql.DatabaseError as e:
        print(f"Error querying data: {e}")
        return pd.DataFrame()

def main():
    db_path = '../database/sukayu_historical_obs_daily.sqlite'
    
    # Connect to the SQLite database
    conn = connect_to_db(db_path)
    if conn is None:
        return

    # Query all data
    df = query_all_data(conn)
    if df.empty:
        conn.close()
        return

    # Write the data to a tab-delimited CSV file
    output_csv_path = '../outputs/database.csv'
    df.to_csv(output_csv_path, sep='\t', index=False)

    # Zip the CSV file
    output_zip_path = '../outputs/database.zip'
    with zipfile.ZipFile(output_zip_path, 'w') as zipf:
        zipf.write(output_csv_path, os.path.basename(output_csv_path))

    # Remove the original CSV file after zipping
    os.remove(output_csv_path)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()