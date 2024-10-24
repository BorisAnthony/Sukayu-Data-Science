import os
import sys
import shutil
import sqlite3

# Add the parent directory of utils.py to the system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from f_database import (
    column_exists,
    create_new_table,
    copy_data_to_new_table,
    update_rows,
    calculate_and_update_rolling_averages,
    delete_extra_tables
    )


def expand_database():


    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))


    # Define paths relative to the script's directory
    src_db_path  = os.path.join(script_dir, '../../database/src/sukayu_historical_obs_daily.sqlite')
    dest_db_path = os.path.join(script_dir, '../../database/src/sukayu_historical_obs_daily_expanded.sqlite')

    
    # Step 1: Make a working copy of the database
    shutil.copyfile(src_db_path, dest_db_path)
    
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
    
    # Step 6a: Loop through the new table and update rows
    update_rows(cursor)

    # Step 6b: Calculate and update 7-day centered rolling averages
    calculate_and_update_rolling_averages(cursor)
    
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
    
    pass


if __name__ == "__main__":
    expand_database()