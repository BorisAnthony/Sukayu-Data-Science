import sqlite3
import pandas as pd
import json
import pytz
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

def get_timespan_data(df, year, timezone, date_column, find_last=False, custom_start_month=1, custom_start_day=1):
    """
    Extract timespan data for a given year and date range.
    """
    next_year = year + 1
    later_first = pd.Timestamp(f'{next_year}-07-01', tz=timezone)
    
    if custom_start_month == 1 and custom_start_day == 1:
        earlier_first = pd.Timestamp(f'{next_year}-01-01', tz=timezone)
    else:
        earlier_first = pd.Timestamp(f'{year}-{custom_start_month:02d}-{custom_start_day:02d}', tz=timezone)
    
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

def main():
    db_path = '../database/sukayu_historical_obs_daily.sqlite'
    query = "SELECT obs_date, snowfall, temp_avg, snowdepth FROM obs_sukayu_daily"
    
    # Connect to the SQLite database
    conn = connect_to_db(db_path)
    if conn is None:
        return

    # Query the data
    df = query_data(conn, query)
    if df.empty:
        conn.close()
        return

    # Convert the date column to datetime and set timezone to JST
    df = convert_to_datetime(df, 'obs_date', jst)

    # Clean the snowfall data
    df = clean_column_data(df, 'snowfall')

    # Filter the data to include only dates from September 1st to December 31st
    oct_dec_df = filter_data_by_month(df, 'obs_date', 9, 12)

    # Extract the year
    oct_dec_df = extract_year(oct_dec_df, 'obs_date', 'year')

    # Initialize a dictionary to store the first and last snowfall dates
    snowfall_dates = {}

    # Group the data by year and find the first and last snowfall dates
    for year, group in oct_dec_df.groupby('year'):
   
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
            timespan_data=get_timespan_data(df, year, jst, 'obs_date', find_last=True), 
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
            threshold=0.0, 
            comparison='gt'
            )

        """Find the last date where snowfall is not 0.0 and followed by at least 2 more days with non-zero snowfall."""
        last_snowfall_of_consequence_date = find_in(
            timespan_data=get_timespan_data(df, year, jst, 'obs_date', find_last=True), 
            timespan=3, 
            date_column='obs_date',
            search_column='snowfall', 
            all_or_mean='all', 
            threshold=0.0, 
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
            timespan_data=get_timespan_data(df, year, jst, 'obs_date'), 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='all', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the date when temp_avg has been above 10.0 for 7 consecutive days."""
        scandi_start_of_summer_date = find_in(
            timespan_data=get_timespan_data(df, year, jst, 'obs_date'), 
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
            timespan_data=get_timespan_data(df, year, jst, 'obs_date'), 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the date when the average temp_avg has been above 10.0 for 7 consecutive days."""
        scandi_start_of_summer_avg_based_date = find_in(
            timespan_data=get_timespan_data(df, year, jst, 'obs_date'), 
            timespan=7, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=10.0, 
            comparison='gt'
            )
        
        """Find the date when snowdepth is not 0.0 by scanning backwards from June of the following year."""
        snow_gone_date = find_in(
            timespan_data=get_timespan_data(df, year, jst, 'obs_date', find_last=True), 
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
        snowdepth_timespan = get_timespan_data(df, year, jst, 'obs_date', custom_start_month=11, custom_start_day=1)
        depths = [100, 200, 300, 400, 500]
        snowdepth_date = {}

        for depth in depths:
            snowdepth_date[depth] = find_in(
                timespan_data=snowdepth_timespan, 
                timespan=1, 
                date_column='obs_date', 
                search_column='snowdepth', 
                all_or_mean='any', 
                threshold=depth,
                comparison='ge'
            )

        """ Return the highest value of snowdepth for the season """
        max_snowdepth = snowdepth_timespan['snowdepth'].max()
        max_snowdepth = None if pd.isna(max_snowdepth) else max_snowdepth

        """ Calculate total snowfall for the season """
        total_snowfall = snowdepth_timespan['snowfall'].sum()


        next_year_abb = str(int(year) + 1)[-2:]
        season_label = f"{year}-{next_year_abb}"
        # print(season_label)

        # Store the dates in the dictionary
        snowfall_dates[season_label] = {
            'snowfalls': {
                'first': first_snowfall_date,
                'last': last_snowfall_date,
                'first_of_consequence': first_snowfall_of_consequence_date,
                'last_of_consequence': last_snowfall_of_consequence_date,
                'total': total_snowfall
            },
            'snowdepths': {
                'max': max_snowdepth,
                **{f'{depth}': snowdepth_date[depth] for depth in depths},
                'all_gone': snow_gone_date

            },
            'scandi_season_starts': {
                'autumn_all': scandi_start_of_autumn_date,
                'winter_all': scandi_start_of_winter_date,
                'spring_all': scandi_start_of_spring_date,
                'summer_all': scandi_start_of_summer_date,
                'autumn_avg': scandi_start_of_autumn_avg_based_date,
                'winter_avg': scandi_start_of_winter_avg_based_date,
                'spring_avg': scandi_start_of_spring_avg_based_date,
                'summer_avg': scandi_start_of_summer_avg_based_date
            }
        }

    # Write the dates to a JSON file
    with open('../outputs/Sukayu-Seasonal-Dates.json', 'w') as file:
        json.dump(snowfall_dates, file, indent=2)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()