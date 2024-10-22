import os
import zipfile
import pytz
import sqlite3
import pandas as pd
import json
import operator

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


def main():

    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define paths relative to the script's directory
    db_path = os.path.join(script_dir, '../database/sukayu_historical_obs_daily.sqlite')
    output_path = os.path.join(script_dir, '../outputs')
    cl_path = os.path.join(script_dir, '../Citation_and_License.md')

    # query = "SELECT obs_date, temp_avg, temp_hgh, temp_low, temp_amp, snowfall, snowdepth, wind_speed_amp FROM obs_sukayu_daily"
    query = """
    SELECT obs_date, temp_avg, temp_hgh, temp_low, 
        calc_amplitude(temp_hgh, temp_low) AS temp_amp, 
        snowfall, snowdepth, 
        calc_amplitude(wind_gust_speed, wind_avg_speed) AS wind_speed_amp 
    FROM obs_sukayu_daily
    """

    # Connect to the SQLite database
    conn = connect_to_db(db_path)
    if conn is None:
        return
    print("DB   - Connection established")
    

    # Create a cursor object
    cursor = conn.cursor()

    # Register the UDF with the SQLite connection
    conn.create_function("calc_amplitude", 2, calc_amplitude)

    # Query the data
    df = query_data(conn, query)
    if df.empty:
        conn.close()
        return
    print("DB   - Data queried")


    # Convert the date column to datetime and set timezone to JST
    df = convert_to_datetime(df, 'obs_date', jst)

    # Clean the snowfall data
    # df = clean_column_data(df, 'snowfall')

    # Filter the data to include only dates from September 1st to December 31st
    oct_dec_df = filter_data_by_month(df, 'obs_date', 9, 12)

    # Extract the year
    oct_dec_df = extract_year(oct_dec_df, 'obs_date', 'year')

    # Initialize a dictionary to store the first and last snowfall dates
    snowfall_dates = {}

    # Group the data by year and find the first and last snowfall dates
    for year, group in oct_dec_df.groupby('year'):

        """ Set the timespan data for the season for use in full-season min/max/sum/avg calculations """
        season_timespan = get_timespan_data(df, year, 'obs_date', custom_start_month=11, custom_start_day=1)

        """Find the first date where snowfall is not 0.0."""
        first_snowfall_date = find_in(
            timespan_data=group, 
            timespan=1, 
            date_column='obs_date', 
            search_column='snowfall',
            all_or_mean='any', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the last snowfall date by scanning backwards from June of the following year."""
        last_snowfall_date  = find_in(
            timespan_data=get_timespan_data(df, year, 'obs_date', find_last=True), 
            timespan=1, 
            date_column='obs_date', 
            search_column='snowfall', 
            all_or_mean='any', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the first date where snowfall is not 0.0 and followed by at least 2 more days with non-zero snowfall."""
        first_snowfall_of_consequence_date = find_in(
            timespan_data=group, 
            timespan=3, 
            date_column='obs_date', 
            search_column='snowfall', 
            all_or_mean='all', 
            threshold=5.0, 
            comparison='gt'
            )

        """Find the last date where snowfall is not 0.0 and followed by at least 2 more days with non-zero snowfall."""
        last_snowfall_of_consequence_date = find_in(
            timespan_data=get_timespan_data(df, year, 'obs_date', find_last=True), 
            timespan=3, 
            date_column='obs_date',
            search_column='snowfall', 
            all_or_mean='all', 
            threshold=5.0, 
            comparison='gt'
            )
        # Snowfall total is calculated at the end of this list
        

        """Find the date when temp_avg has been below 10.0 for 7 consecutive days."""
        scandi_start_of_autumn_date = find_in(
            timespan_data=group, 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='all', 
            threshold=10.0, 
            comparison='lt'
            )

        """Find the date when temp_avg has been below 0.0 for 7 consecutive days."""
        scandi_start_of_winter_date = find_in(
            timespan_data=group, 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='all', 
            threshold=0.0, 
            comparison='lt'
            )

        """Find the date when temp_avg has been above 0.0 for 7 consecutive days."""
        scandi_start_of_spring_date = find_in(
            timespan_data=get_timespan_data(df, year, 'obs_date'), 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='all', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the date when temp_avg has been above 10.0 for 7 consecutive days."""
        scandi_start_of_summer_date = find_in(
            timespan_data=get_timespan_data(df, year, 'obs_date'), 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='all', 
            threshold=10.0, 
            comparison='gt'
            )
        
        """Find the date when the average temp_avg has been below 10.0 for 7 consecutive days."""
        scandi_start_of_autumn_avg_based_date = find_in(
            timespan_data=group, 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=10.0, 
            comparison='lt'
            )

        """Find the date when the average temp_avg has been below 0.0 for 7 consecutive days."""
        scandi_start_of_winter_avg_based_date = find_in(
            timespan_data=group, 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=0.0, 
            comparison='lt'
            )

        """Find the date when the average temp_avg has been above 0.0 for 7 consecutive days."""
        scandi_start_of_spring_avg_based_date = find_in(
            timespan_data=get_timespan_data(df, year, 'obs_date'), 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the date when the average temp_avg has been above 10.0 for 7 consecutive days."""
        scandi_start_of_summer_avg_based_date = find_in(
            timespan_data=get_timespan_data(df, year, 'obs_date'), 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=10.0, 
            comparison='gt'
            )
        
        """Find the date when snowdepth is not 0.0 by scanning backwards from June of the following year."""
        snow_gone_date = find_in(
            timespan_data=get_timespan_data(df, year, 'obs_date', find_last=True), 
            timespan=1, 
            date_column='obs_date', 
            search_column='snowdepth', 
            all_or_mean='any', 
            threshold=0.0, 
            comparison='gt',
            add_days=1
            )

        # SNOWDEPTHS

        """Find the dates when snowdepths first reach 100, 200, 300, 400, and 500 cm."""
        depths = [100, 200, 300, 400, 500]
        snowdepth_date = {}

        for depth in depths:
            snowdepth_date[depth] = find_in(
                timespan_data=season_timespan, 
                timespan=1, 
                date_column='obs_date', 
                search_column='snowdepth', 
                all_or_mean='any', 
                threshold=depth,
                comparison='ge'
            )

        """ Return the highest value of snowdepth for the season """
        max_snowdepth = season_timespan['snowdepth'].max()
        max_snowdepth = None if pd.isna(max_snowdepth) else max_snowdepth

        """ Calculate total snowfall for the season """
        total_snowfall = season_timespan['snowfall'].sum()

        """ Calculate the count of instances where snowfall was >= 10 """
        snowfall_levels = [10, 20, 30, 40, 50, 60, 70, 80]
        snowfall_level_counts = {}
        for level in snowfall_levels:
            snowfall_level_counts[level] = int((season_timespan['snowfall'] >= level).sum())


        """ Return the highest & lowest values of temp_hgh & temp_low for the season """
        max_temp_avg = season_timespan['temp_avg'].max()
        max_temp_avg = None if pd.isna(max_temp_avg) else max_temp_avg
        min_temp_avg = season_timespan['temp_avg'].min()
        min_temp_avg = None if pd.isna(min_temp_avg) else min_temp_avg
        avg_temp_avg = season_timespan['temp_avg'].mean()
        avg_temp_avg = None if pd.isna(avg_temp_avg) else round(avg_temp_avg, 1)

        max_temp_hgh = season_timespan['temp_hgh'].max()
        max_temp_hgh = None if pd.isna(max_temp_hgh) else max_temp_hgh
        min_temp_hgh = season_timespan['temp_hgh'].min()
        min_temp_hgh = None if pd.isna(min_temp_hgh) else min_temp_hgh
        avg_temp_hgh = season_timespan['temp_hgh'].mean()
        avg_temp_hgh = None if pd.isna(avg_temp_hgh) else round(avg_temp_hgh, 1)

        max_temp_low = season_timespan['temp_low'].max()
        max_temp_low = None if pd.isna(max_temp_low) else max_temp_low
        min_temp_low = season_timespan['temp_low'].min()
        min_temp_low = None if pd.isna(min_temp_low) else min_temp_low
        avg_temp_low = season_timespan['temp_low'].mean()
        avg_temp_low = None if pd.isna(avg_temp_low) else round(avg_temp_low, 1)

        max_temp_amp = season_timespan['temp_amp'].max()
        max_temp_amp = None if pd.isna(max_temp_amp) else max_temp_amp
        min_temp_amp = season_timespan['temp_amp'].min()
        min_temp_amp = None if pd.isna(min_temp_amp) else min_temp_amp
        avg_temp_amp = season_timespan['temp_amp'].mean()
        avg_temp_amp = None if pd.isna(avg_temp_amp) else round(avg_temp_amp, 1)

        max_ws_amp = season_timespan['wind_speed_amp'].max()
        max_ws_amp = None if pd.isna(max_ws_amp) else max_ws_amp
        min_ws_amp = season_timespan['wind_speed_amp'].min()
        min_ws_amp = None if pd.isna(min_ws_amp) else min_ws_amp
        avg_ws_amp = season_timespan['wind_speed_amp'].mean()
        avg_ws_amp = None if pd.isna(avg_ws_amp) else round(avg_ws_amp, 1)


        next_year_abb = str(int(year) + 1)[-2:]
        season_label = f"{year}-{next_year_abb}"
        # print(season_label)

        # Store the dates in the dictionary
        snowfall_dates[season_label] = {
            'snowfalls': {
                'fst': first_snowfall_date,
                'lst': last_snowfall_date,
                'fst_subs': first_snowfall_of_consequence_date,
                'lst_subs': last_snowfall_of_consequence_date,
                'total': total_snowfall,
                'days_over': {
                    **{f'{level}': snowfall_level_counts[level] for level in snowfall_levels}
                }
            },
            'snowdepths': {
                'max': max_snowdepth,
                **{f'{depth}': snowdepth_date[depth] for depth in depths},
                'fin': snow_gone_date

            },
            'scandi_season_starts': {
                'strict' : {
                    'aut': scandi_start_of_autumn_date,
                    'win': scandi_start_of_winter_date,
                    'spr': scandi_start_of_spring_date,
                    'sum': scandi_start_of_summer_date
                },
                'avg_based': {
                    'aut': scandi_start_of_autumn_avg_based_date,
                    'win': scandi_start_of_winter_avg_based_date,
                    'spr': scandi_start_of_spring_avg_based_date,
                    'sum': scandi_start_of_summer_avg_based_date
                }
            },
            'temps': {
                'avg': {
                    'avg': avg_temp_avg,
                    'max': max_temp_avg,
                    'min': min_temp_avg
                },
                'hgh': {
                    'avg': avg_temp_hgh,
                    'max': max_temp_hgh,
                    'min': min_temp_hgh
                },
                'low': {
                    'avg': avg_temp_low,
                    'max': max_temp_low,
                    'min': min_temp_low
                },
                'amp': {
                    'avg': avg_temp_amp,
                    'max': max_temp_amp,
                    'min': min_temp_amp
                }
            },
            'winds': {
                'amp': {
                    'avg': avg_ws_amp,
                    'max': max_ws_amp,
                    'min': min_ws_amp
                }
            }
        }
    print("DATA - Processing complete")

    # Write the dates to a JSON file
    output_path_derived = os.path.join(output_path, 'derived')
    output_json_path = os.path.join(output_path_derived, 'Sukayu-Winters-Data.json')
    with open(output_json_path, 'w') as file:
        json.dump(snowfall_dates, file, indent=2)
        print(f"JSON - DATA - File written")


    # Flatten the snowfall_dates dictionary
    flattened_data = {season: flatten_dict(data) for season, data in snowfall_dates.items()}

    # Convert the flattened dictionary to a DataFrame
    df_snowfall_dates = pd.DataFrame.from_dict(flattened_data, orient='index')

    # Write the DataFrame to a CSV file and zip it
    write_and_zip_csv(data=df_snowfall_dates, filename='Sukayu-Winters-Data', output_path=output_path_derived, label='DATA -', include_file_path=cl_path)



    # __________________________________________________________________________
    # MARK: DB Cleanup
    # Run an integrity check
    cursor.execute("PRAGMA integrity_check;")
    integrity_result = cursor.fetchone()
    if integrity_result[0] != 'ok':
        print("Integrity check failed. Aborting.")
        conn.close()
        return
    print("DB   - Integrity check passed")
    
    # Run an optimization
    cursor.execute("PRAGMA optimize;")
    print("DB   - Optimization complete")

    # Commit any transactions
    conn.commit()

    # Compact the database
    cursor.execute("VACUUM main;")
    cursor.execute("VACUUM temp;")
    print("DB   - Vacuum complete")




    # __________________________________________________________________________
    # MARK: DB Dump to Zipped CSV
    # Dump a CSV export of the whole dataset
    # Query the data
    dump_query = "SELECT * FROM obs_sukayu_daily"
    df_dump = query_data(conn, dump_query)
    if df_dump.empty:
        conn.close()
        return
    output_path_jma = os.path.join(output_path, 'jma')
    # Write the data to a tab-delimited CSV file and zip it
    write_and_zip_csv(data=df_dump, filename='sukayu_historical_obs_daily', output_path=output_path_jma, label='DB -', include_file_path=cl_path)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()