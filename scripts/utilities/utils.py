import os
import pandas as pd
import sqlite3
import operator
import pytz
import zipfile
from datetime import datetime

from typing import Dict, Union, List

# Define the JST timezone
jst = pytz.timezone('Asia/Tokyo')

# ______________________________________________________________________________
# MARK: DB Functions

def db_connect(db_path):
    """Connect to the SQLite database."""
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        # conn.close()
        return

def db_query(query, conn):
    """Query the data from the database."""
    try:
        return pd.read_sql_query(query, conn)
    except pd.io.sql.DatabaseError as e:
        print(f"Error querying data: {e}")
        conn.close()
        return

def db_column_exists(cursor, table_name, column_name):
    """Check if a column exists in a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in cursor.fetchall()]
    return column_name in columns

def db_delete_extra_tables(cursor, pattern='sqlite_stat'):
    """Delete any tables other than obs_sukayu_daily."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        # if table[0] != 'obs_sukayu_daily':
        if pattern in table[0]:
            cursor.execute(f"DROP TABLE {table[0]}")

def db_pragma_integrity_check(cursor):
    """Run an integrity check on the database."""
    cursor.execute("PRAGMA integrity_check;")
    integrity_result = cursor.fetchone()
    if integrity_result[0] != 'ok':
        print("Integrity check failed. Aborting.")
        # conn.close()
        return

def db_compact_database(cursor):
    """Compact the database to reduce file size."""
    cursor.execute("VACUUM main;")
    cursor.execute("VACUUM temp;")


def df_convert_to_datetime(df, column, timezone):
    """Convert the date column to datetime and set timezone."""
    df[column] = pd.to_datetime(df[column])
    if df[column].dt.tz is None:
        df[column] = df[column].dt.tz_localize('UTC').dt.tz_convert(timezone)
    else:
        df[column] = df[column].dt.tz_convert(timezone)
    return df

# ! unneeded ?
# def clean_column_data(df, column):
#     """Convert the column to numeric and drop NaN values."""
#     df[column] = pd.to_numeric(df[column], errors='coerce')
#     df.dropna(subset=[column], inplace=True)
#     return df




def df_get_timespan_data(
    df, 
    date_column,
    start_month=9,  # Changed default to September
    end_month=5,    # Changed default to May
    year=None,
    custom_start_day=1,
    find_last=False
):
    """
    Filter dataframe to include only dates within specified timespan.
    Handles academic/fiscal year spans that cross calendar years.
    
    Parameters:
    -----------
        df : pandas.DataFrame
            Input dataframe containing date data
        date_column : str
            Name of the column containing datetime data
        start_month : int, default 9
            Starting month (1-12). Default is 9 (September)
        end_month : int, default 5
            Ending month (1-12). Default is 5 (May)
            If end_month < start_month, assumes crossing into next year
            (e.g., Sept 2023 to May 2024)
        year : int, optional
            Starting year for the period. If None, filters by month numbers only.
            For academic years, this is typically the earlier year
            (e.g., 2023 for 2023-2024 academic year)
        custom_start_day : int, default 1
            Day of the month to start from
        find_last : bool, default False
            If True, sorts results in descending order
        
    Returns:
    --------
        pandas.DataFrame
            Filtered dataframe sorted by date_column
        
    Examples:
    --------
        # Get data for 2023-2024 academic year (Sept 2023 to May 2024)
        df_2023_24 = get_timespan_data(df, 'date', year=2023)  # uses default Sept-May
        
        # Get data for 2023-2024 custom period (Oct 2023 to Apr 2024)
        df_custom = get_timespan_data(df, 'date', start_month=10, end_month=4, year=2023)
        
        # Get all Oct-Apr periods across multiple years
        df_all_periods = get_timespan_data(df, 'date', start_month=10, end_month=4)  # year=None
    """
    if year is None:
        # Filter by month numbers only, handling wrap-around case
        if end_month < start_month:
            # For cases like Sept(9)-May(5), we want months >= 9 OR <= 5
            mask = (df[date_column].dt.month >= start_month) | \
                   (df[date_column].dt.month <= end_month)
        else:
            # Normal case like Mar(3)-Jun(6)
            mask = (df[date_column].dt.month >= start_month) & \
                   (df[date_column].dt.month <= end_month)
    else:
        # Create timestamp bounds spanning to next year if needed
        start_date = pd.Timestamp(f'{year}-{start_month:02d}-{custom_start_day:02d}', tz='UTC')
        
        # If end_month is less than start_month, it means we're going into next year
        next_year = year + 1 if end_month < start_month else year
        end_date = pd.Timestamp(f'{next_year}-{end_month:02d}-01', tz='UTC')
        
        mask = (df[date_column] >= start_date) & (df[date_column] <= end_date)
    
    timespan_data = df[mask].copy()
    # return timespan_data.sort_values(by=date_column, ascending=not find_last)
    
    return timespan_data


# def get_timespan_data(df, year, date_column, find_last=False, custom_start_month=1, custom_start_day=1):
#     """
#     Extract timespan data for a given year and date range.
#     """
#     next_year = year + 1
#     later_first = pd.Timestamp(f'{next_year}-07-01', tz=jst)
    
#     if custom_start_month == 1 and custom_start_day == 1:
#         earlier_first = pd.Timestamp(f'{next_year}-01-01', tz=jst)
#     else:
#         earlier_first = pd.Timestamp(f'{year}-{custom_start_month:02d}-{custom_start_day:02d}', tz=jst)
    
#     timespan_data = df[(df[date_column] >= earlier_first) & (df[date_column] <= later_first)]
#     return timespan_data.sort_values(by=date_column, ascending=not find_last)


def df_find_in(df_timespan_data, dayspan, date_column, search_column, all_or_mean, threshold, comparison, offset=0):
    """Find the date based on the specified condition.
    
    Parameters:
        df_timespan_data: DataFrame sorted by date ascending
        dayspan: int, number of consecutive days to check
        date_column: str, name of date column
        search_column: str, name of column to check values in
        all_or_mean: str, how to evaluate the span - "all", "mean", or "any"
        threshold: numeric, value to compare against
        comparison: str, operator comparison name (e.g. "gt", "lt", "ge")
        chronological_scan: bool, if True scan from earliest date, if False from latest
        dayspan_direction: str, "following" or "preceding", direction to check days
    """
    op = getattr(operator, comparison)
        
    for i in range(len(df_timespan_data) - max(0, dayspan - 1)):

        to_compare_data = df_timespan_data[search_column].iloc[i:i+max(1, dayspan)]            
        to_return_data = df_timespan_data[date_column].iloc[i]
        
        condition_met = False
        if all_or_mean == "all":
            condition_met = all(op(value, threshold) for value in to_compare_data)
        elif all_or_mean == "mean":
            condition_met = op(to_compare_data.mean(), threshold)
            # If mean condition is met, adjust return date to middle of span
            if condition_met:
                middle_offset = (dayspan - 1) // 2  # Integer division for odd spans
                to_return_data = df_timespan_data[date_column].iloc[i + middle_offset]
        else:  # "any" or single value case
            condition_met = op(to_compare_data.iloc[0], threshold)
        
        if condition_met:
            to_return_data = to_return_data + pd.DateOffset(days=offset)
            return to_return_data.strftime('%Y-%m-%d')
    
    return None  # Return None if no condition is met






""" Flatten the dictionary before trying to Panda it into CSV """
def dict_flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(dict_flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def dict_flatten_seasons_data(seasons_data):
    flattened_data = []
    for season, data in seasons_data.items():
        flat_data = {'season': season}  # Assign "season" first
        flat_data.update(dict_flatten(data))  # Update with flattened data
        flattened_data.append(flat_data)
    return flattened_data




def df_write_to_sqlite(data, table_name, sqlite_conn):
    """
    Write data to a SQLite database table.
    """
    # Write the data to the SQLite table
    data.to_sql(table_name, con=sqlite_conn, if_exists='replace', index=False)
    print(f"SQLite - Table '{table_name}' written successfully")



def write_and_zip_csv(data, filename, output_path, include_file_path=None, label='', sep='\t', index=True):
    """
    Write data to a tab-delimited CSV file, zip it, and remove the original CSV file.
    """
    # Write the data to a tab-delimited CSV file
    output_csv_path = os.path.join(output_path, f'{filename}.csv')
    data.to_csv(output_csv_path, sep=sep, index=index)
    print(f"CSV  - {label} File written")

    # Zip the CSV file
    output_zip_path = os.path.join(output_path, f'{filename}.csv.zip')
    with zipfile.ZipFile(output_zip_path, 'w') as zipf:
        zipf.write(output_csv_path, os.path.basename(output_csv_path))

        if include_file_path is not None:
            if os.path.exists(include_file_path):
                zipf.write(include_file_path, os.path.basename(include_file_path))
                print(f"ZIP  - {label} Extra file included")
            else:
                print(f"ZIP  - {label} Extra file not found")
        
        print(f"ZIP  - {label} File written")

    # Remove the original CSV file after zipping
    os.remove(output_csv_path)
    print(f"CSV  - {label} Original removed")




def calculate_stats(df: pd.DataFrame, columns: List[str], round_digits: int = 1) -> Dict[str, Dict[str, Union[float, None]]]:
    """
    Calculate min, max, and average statistics for specified columns in a DataFrame.
    Handles NaN values and rounds averages to specified decimal places.
    
    Args:
        df: pandas DataFrame containing the data
        columns: list of column names to process
        round_digits: number of decimal places to round averages to (default: 1)
        
    Returns:
        Dictionary with statistics for each column:
        {
            'column_name': {
                'max': float or None,
                'min': float or None,
                'avg': float or None
            }
        }
    """
    stats = {}
    
    for col in columns:
        max_val = df[col].max()
        min_val = df[col].min()
        avg_val = df[col].mean()
        
        stats[col] = {
            'max': None if pd.isna(max_val) else max_val,
            'min': None if pd.isna(min_val) else min_val,
            'avg': None if pd.isna(avg_val) else round(avg_val, round_digits)
        }
    
    return stats



def get_this_winter_season_info():
    # Get current date
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    
    # Determine winter season years
    if 1 <= current_month <= 6:
        # First half of the year: winter season is previous year to current year
        start_year = current_year - 1
        end_year = current_year
    else:
        # Second half of the year: winter season is current year to next year
        start_year = current_year
        end_year = current_year + 1
    
    # Format the season string (YYYY-YY)
    season_string = f"{start_year}-{str(end_year)[2:]}"
    
    # Determine meteorological start dates for Spring and Summer
    spring_start = f"{end_year}-03-01"
    summer_start = f"{end_year}-06-01"
    
    # Return all requested information in a dictionary with named keys
    return {
        "winter_label": season_string,
        "winter_start_year": start_year,
        "winter_end_year": end_year,
        "spring_start": spring_start,
        "summer_start": summer_start
    }

def is_after_month_day(date, check_month, check_day):
    """
    Check if the given date is after May 17th (ignoring the year).
    
    Args:
        date: A datetime object to check.
        check_month: The month to check against (5 for May).
        check_day: The day to check against (17 for May 17th).
        
    Returns:
        bool: True if the date is after e.g. May 17th, False otherwise.
    """
    # Extract month and day from the input date
    month = date.month
    day = date.day
    
    # Check if the date is after check_month/check_day
    if month > check_month or (month == check_month and day > check_day):
        return True
    else:
        return False