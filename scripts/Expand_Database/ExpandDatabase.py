import shutil

import os
import sys
# ../../utilities
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utilities"))
# Import modules in scripts/utilities/
from paths import (PROJECT_ROOT, SCRIPTS_DIR, UTILITIES_DIR, DATABASE_DIR, DATABASE_PATH, OUTPUTS_DIR, FIGURES_DIR, DERIVED_DIR)
from utils import (
    db_connect,
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

    # Define paths relative to the script's directory
    src_db_path  = os.path.join(DATABASE_DIR, 'src/sukayu_historical_obs_daily.sqlite')
    dest_db_path = os.path.join(DATABASE_DIR, 'src/sukayu_historical_obs_daily_expanded.sqlite')

    
    # Make a working copy of the database
    shutil.copyfile(src_db_path, dest_db_path)
    
    # Connect to the SQLite database
    conn = db_connect(dest_db_path)
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