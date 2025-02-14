import os
import sys
import shutil
import sqlite3

# Add the parent directory of utils.py to the system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Add the utilities directory to the system path
script_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(script_dir, '../utilities')
sys.path.append(utilities_dir)

from utils import (
    db_connect,
    db_query_data,
    db_column_exists,
    db_delete_extra_tables,
    db_pragma_integrity_check,
    db_compact_database
    )

from database_ops import (
    create_new_table,
    copy_data_to_new_table,
    update_rows,
    calculate_and_update_rolling_averages
    )


def expand_database():

    print("\n\nSCRIPT: EXPAND DATABASE ----------------\n")

    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))


    # Define paths relative to the script's directory
    src_db_path  = os.path.join(script_dir, '../../database/src/sukayu_historical_obs_daily.sqlite')
    db_path = os.path.join(script_dir, '../../database/src/sukayu_historical_obs_daily_expanded.sqlite')

    
    # Make a working copy of the database
    shutil.copyfile(src_db_path, db_path)
    
    # Connect to the SQLite database
    conn = db_connect(db_path)
    print("DB   - Connection established")

    # Create a cursor object
    cursor = conn.cursor()
    
    # Check if the columns exist
    if db_column_exists(cursor, 'obs_sukayu_daily', 'temp_amp') or db_column_exists(cursor, 'obs_sukayu_daily', 'wind_speed_amp'):
        print("Columns 'temp_amp' or 'wind_speed_amp' already exist. Aborting.")
        conn.close()
        return
    
    # Create the new table with the desired column order
    create_new_table(cursor)
    
    # Copy data from the old table to the new table
    copy_data_to_new_table(cursor)
    
    # Loop through the new table and update rows
    update_rows(cursor)

    # Calculate and update 7-day centered rolling averages
    calculate_and_update_rolling_averages(cursor)
    
    # Drop the old table
    cursor.execute("DROP TABLE obs_sukayu_daily")
    
    # Rename the new table to the old table's name
    cursor.execute("ALTER TABLE obs_sukayu_daily_new RENAME TO obs_sukayu_daily")

    # --------------------------------------------------------------------------



    # __________________________________________________________________________
    # MARK: DB Cleanup
    # Run an integrity check
    db_pragma_integrity_check(cursor)
    print("DB   - Integrity check passed")
    
    db_delete_extra_tables(cursor)
    print("DB   - Extra tables deleted")

    # Commit any transactions
    conn.commit()
    
    # Compact the database
    db_compact_database(cursor)
    print("DB   - Vacuum complete")
    
    # --------------------------------------------------------------------------



    # Close the database connection
    conn.close()



    pass



if __name__ == "__main__":
    expand_database()