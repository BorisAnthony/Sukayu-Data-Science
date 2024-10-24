import os
import sys
import pandas as pd
import json
import shutil

# Add the parent directory of utils.py to the system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import (
    connect_to_db,
    query_data,
    convert_to_datetime,
    clean_column_data,
    filter_data_by_month,
    extract_year,
    get_timespan_data,
    find_in,
    calc_amplitude,
    flatten_dict,
    flatten_seasons_data,
    write_to_sqlite,
    write_and_zip_csv,
    delete_extra_tables,
    calculate_stats,
    jst
)


def process_data():


    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))


    # Define paths relative to the script's directory
    src_db_path = os.path.join(script_dir, '../../database/src/sukayu_historical_obs_daily_expanded.sqlite')
    db_path = os.path.join(script_dir, '../../database/dist/sukayu_historical_obs_daily_everything.sqlite')


    # Make a working copy of the database
    shutil.copyfile(src_db_path, db_path)


    output_path = os.path.join(script_dir, '../../outputs')
    cl_path     = os.path.join(script_dir, '../../Citation_and_License.md')



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



    # Filter the data to include only dates from September 1st to December 31st
    oct_dec_df = filter_data_by_month(df, 'obs_date', 9, 12)



    # Extract the year
    oct_dec_df = extract_year(oct_dec_df, 'obs_date', 'year')



    # Initialize a dictionary to store the first and last snowfall dates
    seasons_data = {}



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
        snowdepth_date_first = {}

        for depth in depths:
            snowdepth_date_first[depth] = find_in(
                timespan_data=season_timespan, 
                timespan=1, 
                date_column='obs_date', 
                search_column='snowdepth', 
                all_or_mean='any', 
                threshold=depth,
                comparison='ge'
            )
        depths_backwards = [500, 400, 300, 200, 100]
        snowdepth_date_last = {}
        for depth in depths_backwards:
            snowdepth_date_last[depth] = find_in(
                timespan_data=get_timespan_data(df, year, 'obs_date', find_last=True), 
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
        columns = ['temp_avg', 'temp_hgh', 'temp_low', 'temp_amp', 'wind_speed_amp']
        stats = calculate_stats(season_timespan, columns)

        next_year_abb = str(int(year) + 1)[-2:]
        season_label = f"{year}-{next_year_abb}"
        # print(season_label)

        # Store the dates in the dictionary
        seasons_data[season_label] = {
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
            'snowdepths': {
                'max': max_snowdepth,
                'first': {
                    **{f'{depth}': snowdepth_date_first[depth] for depth in depths},
                },
                'last': {
                    **{f'{depth}': snowdepth_date_last[depth] for depth in depths_backwards},
                },
                'fin': snow_gone_date

            },
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
            'temps': {
                'avg': {
                    'avg': stats['temp_avg']['avg'],
                    'max': stats['temp_avg']['max'],
                    'min': stats['temp_avg']['min']
                },
                'hgh': {
                    'avg': stats['temp_hgh']['avg'],
                    'max': stats['temp_hgh']['max'],
                    'min': stats['temp_hgh']['min']
                },
                'low': {
                    'avg': stats['temp_low']['avg'],
                    'max': stats['temp_low']['max'],
                    'min': stats['temp_low']['min']
                },
                'amp': {
                    'avg': stats['temp_amp']['avg'],
                    'max': stats['temp_amp']['max'],
                    'min': stats['temp_amp']['min']
                }
            },
            'winds': {
                'amp': {
                    'avg': stats['wind_speed_amp']['avg'],
                    'max': stats['wind_speed_amp']['max'],
                    'min': stats['wind_speed_amp']['min']
                }
            }
        }
    print("DATA - Processing complete")



    # Write the dates to a JSON file
    output_path_derived = os.path.join(output_path, 'derived')
    output_json_path = os.path.join(output_path_derived, 'Sukayu-Winters-Data.json')
    with open(output_json_path, 'w') as file:
        json.dump(seasons_data, file, indent=2)
        print(f"JSON - DATA - File written")



    # Flatten the seasons_data dictionary
    flattened_data = flatten_seasons_data(seasons_data)



    # Convert the flattened list of dictionaries to a DataFrame
    df_seasons_data = pd.DataFrame(flattened_data)



    # Write the DataFrame to a CSV file and zip it
    write_and_zip_csv(data=df_seasons_data, filename='Sukayu-Winters-Data', output_path=output_path_derived, label='DATA -', include_file_path=cl_path)



    # Write the DataFrame to an SQLite table
    write_to_sqlite(df_seasons_data, 'sukayu_winters_data', conn)



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



    # Step 11: Delete any extra tables
    delete_extra_tables(cursor)



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

    pass


if __name__ == "__main__":
    process_data()