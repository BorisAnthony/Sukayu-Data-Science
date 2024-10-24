import os
import pandas as pd
import sqlite3
import operator
import pytz
import zipfile

from typing import Dict, Union, List

# Define the JST timezone
jst = pytz.timezone('Asia/Tokyo')


def connect_to_db(db_path):
    """Connect to the SQLite database."""
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def query_data(conn, query):
    """Query the data from the database."""
    try:
        return pd.read_sql_query(query, conn)
    except pd.io.sql.DatabaseError as e:
        print(f"Error querying data: {e}")
        return pd.DataFrame()

def convert_to_datetime(df, column, timezone):
    """Convert the date column to datetime and set timezone."""
    df[column] = pd.to_datetime(df[column])
    if df[column].dt.tz is None:
        df[column] = df[column].dt.tz_localize('UTC').dt.tz_convert(timezone)
    else:
        df[column] = df[column].dt.tz_convert(timezone)
    return df

def clean_column_data(df, column):
    """Convert the column to numeric and drop NaN values."""
    df[column] = pd.to_numeric(df[column], errors='coerce')
    df.dropna(subset=[column], inplace=True)
    return df

def filter_data_by_month(df, column, start_month, end_month):
    """Filter the data to include only dates within the specified months."""
    return df[(df[column].dt.month >= start_month) & (df[column].dt.month <= end_month)].copy()

def extract_year(df, date_column, year_column):
    """Extract the year from the date column."""
    df.loc[:, year_column] = df[date_column].dt.year
    return df

def get_timespan_data(df, year, date_column, find_last=False, custom_start_month=1, custom_start_day=1):
    """
    Extract timespan data for a given year and date range.
    """
    next_year = year + 1
    later_first = pd.Timestamp(f'{next_year}-07-01', tz=jst)
    
    if custom_start_month == 1 and custom_start_day == 1:
        earlier_first = pd.Timestamp(f'{next_year}-01-01', tz=jst)
    else:
        earlier_first = pd.Timestamp(f'{year}-{custom_start_month:02d}-{custom_start_day:02d}', tz=jst)
    
    timespan_data = df[(df[date_column] >= earlier_first) & (df[date_column] <= later_first)]
    return timespan_data.sort_values(by=date_column, ascending=not find_last)

def find_in(timespan_data, timespan, date_column, search_column, all_or_mean, threshold, comparison, add_days=0):
    """Find the date based on the specified condition."""
    op = getattr(operator, comparison)
    
    for i in range(len(timespan_data) - max(0, timespan - 1)):
        to_compare_data = timespan_data[search_column].iloc[i:i+max(1, timespan)]
        to_return_data = timespan_data[date_column].iloc[i] + pd.Timedelta(days=add_days)
        
        condition_met = False
        if all_or_mean == "all":
            condition_met = all(op(value, threshold) for value in to_compare_data)
        elif all_or_mean == "mean":
            condition_met = op(to_compare_data.mean(), threshold)
        else:  # "any" or single value case
            condition_met = op(to_compare_data.iloc[0], threshold)
        
        if condition_met:
            return to_return_data.strftime('%Y-%m-%d')
    
    return None  # Return None if no condition is met



""" UDF for SQLite """
def calc_amplitude(a, b):
    if a is None or b is None:
        return None
    return round(a - b, 1)



""" Flatten the dictionary before trying to Panda it into CSV """
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_seasons_data(seasons_data):
    flattened_data = []
    for season, data in seasons_data.items():
        flat_data = {'season': season}  # Assign "season" first
        flat_data.update(flatten_dict(data))  # Update with flattened data
        flattened_data.append(flat_data)
    return flattened_data




def write_to_sqlite(data, table_name, sqlite_conn):
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

def delete_extra_tables(cursor):
    """Delete any tables other than obs_sukayu_daily."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        # if table[0] != 'obs_sukayu_daily':
        if 'sqlite_stat' in table[0]:
            cursor.execute(f"DROP TABLE {table[0]}")


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
