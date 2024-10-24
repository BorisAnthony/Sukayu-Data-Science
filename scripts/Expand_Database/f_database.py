import pandas as pd


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
            temp_avg REAL,
            temp_avg_7dcra REAL,
            temp_avg_7dcra_std REAL,
            temp_hgh REAL,
            temp_hgh_7dcra REAL,
            temp_hgh_7dcra_std REAL,
            temp_low REAL,
            temp_low_7dcra REAL,
            temp_low_7dcra_std REAL,
            temp_amp REAL,
            wind_avg_speed REAL,
            wind_max_speed REAL,
            wind_max_dir REAL,
            wind_gust_speed REAL,
            wind_gust_dir REAL,
            wind_avg_dir REAL,
            wind_speed_amp REAL,
            sunshine REAL,
            snowfall REAL,
            snowdepth REAL,
            prec_total REAL,
            prec_max_1x REAL,
            prec_max_10m REAL,
            hum_avg REAL,
            hum_min REAL
        )
    """)

def copy_data_to_new_table(cursor):
    """Copy data from the old table to the new table."""
    cursor.execute("""
        INSERT INTO obs_sukayu_daily_new (
            obs_date,
            obs_time,
            temp_avg,
            temp_avg_7dcra,
            temp_avg_7dcra_std,
            temp_hgh,
            temp_hgh_7dcra,
            temp_hgh_7dcra_std,
            temp_low,
            temp_low_7dcra,
            temp_low_7dcra_std,
            temp_amp,
            wind_avg_speed,
            wind_max_speed,
            wind_max_dir,
            wind_gust_speed,
            wind_gust_dir,
            wind_avg_dir,
            wind_speed_amp,
            sunshine,
            snowfall,
            snowdepth,
            prec_total,
            prec_max_1x,
            prec_max_10m,
            hum_avg,
            hum_min
        )
        SELECT
            obs_date,
            obs_time,
            temp_avg,
            NULL,
            NULL,
            temp_hgh,
            NULL,
            NULL,
            temp_low,
            NULL,
            NULL,
            NULL,
            wind_avg_speed,
            wind_max_speed,
            wind_max_dir,
            wind_gust_speed,
            wind_gust_dir,
            wind_avg_dir,
            NULL,
            sunshine,
            snowfall,
            snowdepth,
            prec_total,
            prec_max_1x,
            prec_max_10m,
            hum_avg,
            hum_min
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


def calculate_and_update_rolling_averages(cursor):
    """Calculate and update 7-day centered rolling averages for temp_avg, temp_hgh, and temp_low."""
    # Load data into a DataFrame
    df = pd.read_sql_query("SELECT rowid, obs_date, temp_avg, temp_hgh, temp_low FROM obs_sukayu_daily_new", cursor.connection)
    
    # Calculate centered rolling Averages with a window of 7 days and min_periods set to 1
    df['temp_avg_7dcra'] = round(df['temp_avg'].rolling(window=7, center=True, min_periods=1).mean(), 1)
    df['temp_hgh_7dcra'] = round(df['temp_hgh'].rolling(window=7, center=True, min_periods=1).mean(), 1)
    df['temp_low_7dcra'] = round(df['temp_low'].rolling(window=7, center=True, min_periods=1).mean(), 1)
    # Calculate centered rolling Standard Deviations with a window of 7 days and min_periods set to 1
    df['temp_avg_7dcra_std'] = round(df['temp_avg'].rolling(window=7, center=True, min_periods=1).std(), 2)
    df['temp_hgh_7dcra_std'] = round(df['temp_hgh'].rolling(window=7, center=True, min_periods=1).std(), 2)
    df['temp_low_7dcra_std'] = round(df['temp_low'].rolling(window=7, center=True, min_periods=1).std(), 2)
    
    # Update the database with the new values
    for index, row in df.iterrows():
        cursor.execute("""
            UPDATE obs_sukayu_daily_new
            SET 
              temp_avg_7dcra = ?, temp_hgh_7dcra = ?, temp_low_7dcra = ?,
              temp_avg_7dcra_std = ?, temp_hgh_7dcra_std = ?, temp_low_7dcra_std = ?
            WHERE rowid = ?
        """, (row['temp_avg_7dcra'], row['temp_hgh_7dcra'], row['temp_low_7dcra'], row['temp_avg_7dcra_std'], row['temp_hgh_7dcra_std'], row['temp_low_7dcra_std'], row['rowid']))







def delete_extra_tables(cursor):
    """Delete any tables other than obs_sukayu_daily."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        if table[0] != 'obs_sukayu_daily':
            cursor.execute(f"DROP TABLE {table[0]}")