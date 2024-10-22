import sqlite3
import shutil
import os

def copy_database(src_path, dest_path):
    """Copy the database to a new file."""
    shutil.copyfile(src_path, dest_path)

def column_exists(cursor, table_name, column_name):
    """Check if a column exists in a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    return column_name in columns

def create_new_table(cursor):
    """Create a new table with the desired column order."""
    cursor.execute("""
        CREATE TABLE obs_sukayu_daily_new (
            obs_date TEXT UNIQUE,
            obs_time INTEGER UNIQUE,
            prec_total REAL,
            prec_max_1x REAL,
            prec_max_10m REAL,
            temp_avg REAL,
            temp_hgh REAL,
            temp_low REAL,
            temp_amp REAL,
            hum_avg REAL,
            hum_min REAL,
            wind_avg_speed REAL,
            wind_max_speed REAL,
            wind_max_dir REAL,
            wind_gust_speed REAL,
            wind_gust_dir REAL,
            wind_avg_dir REAL,
            wind_speed_amp REAL,
            sunshine REAL,
            snowfall REAL,
            snowdepth REAL
        )
    """)

def copy_data_to_new_table(cursor):
    """Copy data from the old table to the new table."""
    cursor.execute("""
        INSERT INTO obs_sukayu_daily_new (
            obs_date, obs_time, prec_total, prec_max_1x, prec_max_10m, temp_avg, temp_hgh, temp_low, temp_amp, hum_avg, hum_min,
            wind_avg_speed, wind_max_speed, wind_max_dir, wind_gust_speed, wind_gust_dir, wind_avg_dir, wind_speed_amp, sunshine, snowfall, snowdepth
        )
        SELECT
            obs_date, obs_time, prec_total, prec_max_1x, prec_max_10m, temp_avg, temp_hgh, temp_low, NULL, hum_avg, hum_min,
            wind_avg_speed, wind_max_speed, wind_max_dir, wind_gust_speed, wind_gust_dir, wind_avg_dir, NULL, sunshine, snowfall, snowdepth
        FROM obs_sukayu_daily
    """)

def update_rows(cursor):
    """Update rows with calculated values."""
    cursor.execute("SELECT rowid, temp_hgh, temp_low, wind_gust_speed, wind_avg_speed FROM obs_sukayu_daily_new")
    rows = cursor.fetchall()
    
    for row in rows:
        rowid, temp_hgh, temp_low, wind_gust_speed, wind_avg_speed = row
        temp_amp = None if None in (temp_hgh, temp_low) else round((temp_hgh - temp_low), 1)
        wind_speed_amp = None if None in (wind_gust_speed, wind_avg_speed) else round((wind_gust_speed - wind_avg_speed), 1)
        
        cursor.execute(
            "UPDATE obs_sukayu_daily_new SET temp_amp = ?, wind_speed_amp = ? WHERE rowid = ?",
            (temp_amp, wind_speed_amp, rowid)
        )

def delete_extra_tables(cursor):
    """Delete any tables other than obs_sukayu_daily."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        if table[0] != 'obs_sukayu_daily':
            cursor.execute(f"DROP TABLE {table[0]}")

def main():
    src_db_path = '../database/sukayu_historical_obs_daily.sqlite'
    dest_db_path = '../database/sukayu_historical_obs_daily-expanded.sqlite'
    
    # Step 1: Make a working copy of the database
    copy_database(src_db_path, dest_db_path)
    
    # Step 2: Open the new "expanded" database
    conn = sqlite3.connect(dest_db_path)
    cursor = conn.cursor()
    
    # Step 3: Check if the columns exist
    if column_exists(cursor, 'obs_sukayu_daily', 'temp_amp') or column_exists(cursor, 'obs_sukayu_daily', 'wind_speed_amp'):
        print("Columns 'temp_amp' or 'wind_speed_amp' already exist. Aborting.")
        conn.close()
        return
    
    # Step 4: Create the new table with the desired column order
    create_new_table(cursor)
    
    # Step 5: Copy data from the old table to the new table
    copy_data_to_new_table(cursor)
    
    # Step 6: Loop through the new table and update rows
    update_rows(cursor)
    
    # Step 7: Drop the old table
    cursor.execute("DROP TABLE obs_sukayu_daily")
    
    # Step 8: Rename the new table to the old table's name
    cursor.execute("ALTER TABLE obs_sukayu_daily_new RENAME TO obs_sukayu_daily")
    
    # Step 9: Run an integrity check
    cursor.execute("PRAGMA integrity_check;")
    integrity_result = cursor.fetchone()
    if integrity_result[0] != 'ok':
        print("Integrity check failed. Aborting.")
        conn.close()
        return
    
    # Step 10: Run an optimization
    cursor.execute("PRAGMA optimize;")
    
    # Step 11: Delete any extra tables
    delete_extra_tables(cursor)

    # Step 12: Commit the transaction
    conn.commit()
    
    # Step 13: Compact the database
    cursor.execute("VACUUM main;")
    cursor.execute("VACUUM temp;")
    
    # Step 14: Close the database connection
    conn.close()
    print("Database expanded, columns reordered, and optimized successfully.")

if __name__ == "__main__":
    main()