import os
import sys
import pandas as pd
import json
import shutil
from datetime import datetime, timedelta

# Add the necessary directories to the system path
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.extend([
    script_dir,
    os.path.join(script_dir, '../utilities')
])

from utils import (
    db_connect,
    db_query,
    df_convert_to_datetime,
    db_delete_extra_tables,
    db_pragma_integrity_check,
    db_compact_database,
    df_get_timespan_data,
    df_find_in,
    dict_flatten_seasons_data,
    df_write_to_sqlite,
    write_and_zip_csv,
    calculate_stats,
    jst,
    get_this_winter_season_info
)


# Get the start and end dates of THIS season
this_season = get_this_winter_season_info()

def process_data():


    print("\n\nSCRIPT: PROCESS DATA -------------------\n")

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
    SELECT
        strftime("%Y", obs_date) as year,
        obs_date, temp_avg, temp_hgh, temp_low, temp_amp, snowfall, snowdepth, wind_speed_amp 
    FROM
        obs_sukayu_daily
    ORDER BY
        obs_date ASC
    """



    # Connect to the SQLite database
    conn = db_connect(db_path)
    print("DB   - Connection established")

    # Create a cursor object
    cursor = conn.cursor()

    # Query the data
    df = db_query(query, conn)
    print("DB   - Data queried")



    # Convert the date column to datetime and set timezone to JST
    df = df_convert_to_datetime(df, 'obs_date', jst)



    # Initialize a dictionary to store the first and last snowfall dates
    seasons_data = {}



    # Group the data by year and find the first and last snowfall dates
    for year, group in df.groupby('year'):

        year = int(year)

        """ Set the timespan data for the season sections to get scandi start dates, snowfall dates, and snowdepth threshold dates """
        df_full_season = df_get_timespan_data(df=df, date_column='obs_date', year=year, start_month=10, end_month=7)
        df_1st_half_se = df_get_timespan_data(df=df, date_column='obs_date', year=year, start_month=10, end_month=1)
        df_2nd_half_se = df_get_timespan_data(df=df, date_column='obs_date', year=year+1, start_month=2, end_month=7)

        """Find the first date where snowfall is not 0.0.
            by scanning the first half of the season, forwards."""
        first_snowfall_date = df_find_in(
            # timespan_data=group, 
            df_timespan_data=df_1st_half_se,
            dayspan=1, 
            date_column='obs_date', 
            search_column='snowfall',
            all_or_mean='any', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the last date where snowfall is not 0.0.
            by scanning the second half of the season, backwards."""
        last_snowfall_date  = df_find_in(
            df_timespan_data=df_2nd_half_se.iloc[::-1], 
            dayspan=1, 
            date_column='obs_date', 
            search_column='snowfall', 
            all_or_mean='any', 
            threshold=0.0, 
            comparison='gt'
            )

        """Find the first date where snowfall was 5.0 or more, and was followed by at least 2 more days with the same
            by scanning the first half of the season, forwards."""
        first_snowfall_of_consequence_date = df_find_in(
            df_timespan_data=df_1st_half_se,
            dayspan=3, 
            date_column='obs_date', 
            search_column='snowfall', 
            all_or_mean='all', 
            threshold=5.0, 
            comparison='ge'
            )

        """Find the last date where snowfall is 5.0 or more, and was preceded by at least 2 more days with the same
            by scanning the second half of the season, backwards."""
        last_snowfall_of_consequence_date = df_find_in(
            df_timespan_data=df_2nd_half_se.iloc[::-1], 
            dayspan=3, 
            date_column='obs_date',
            search_column='snowfall', 
            all_or_mean='all', 
            threshold=5.0, 
            comparison='ge'
            )
        # Snowfall total is calculated at the end of this list
        
        # SCANDI SEASON DATES
        
        scandi_strct_dayspan = 7
        scandi_avg_dayspan = 14
        scandi_warm_threshold = 10.0
        scandi_cold_threshold = 0.0
        
        # ! The Strict Scandi Date is a bit rough. It's just the first date where the average temp is below/above 0.0/10.0 for 7 days.
        # ! The Average-Based Scandi Date is a bit more refined. It's the first date where the average temp is below/above 0.0/10.0 for 14 days.

        # """Find the date when temp_avg started being below 10.0 for {scandi_strct_dayspan} consecutive days
        #     by scanning the first half of the season, forwards."""
        # scandi_start_of_autumn_date = df_find_in(
        #     df_timespan_data=df_1st_half_se,
        #     dayspan=scandi_strct_dayspan, 
        #     date_column='obs_date', 
        #     search_column='temp_avg', 
        #     all_or_mean='all', 
        #     threshold=scandi_warm_threshold, 
        #     comparison='le'
        #     )

        # """Find the date when temp_avg started being below 0.0 for {scandi_strct_dayspan} consecutive days
        #     by scanning the first half of the season, forwards."""
        # scandi_start_of_winter_date = df_find_in(
        #     df_timespan_data=df_1st_half_se,
        #     dayspan=scandi_strct_dayspan, 
        #     date_column='obs_date', 
        #     search_column='temp_avg', 
        #     all_or_mean='all', 
        #     threshold=scandi_cold_threshold, 
        #     comparison='le'
        #     )

        # """Find the date when temp_avg started being above 0.0 for {scandi_strct_dayspan} consecutive days
        #     by scanning the second half of the season, forwards."""
        # scandi_start_of_spring_date = df_find_in(
        #     df_timespan_data=df_2nd_half_se, 
        #     dayspan=scandi_strct_dayspan, 
        #     date_column='obs_date', 
        #     search_column='temp_avg', 
        #     all_or_mean='all', 
        #     threshold=scandi_cold_threshold, 
        #     comparison='ge'
        #     )

        # """Find the date when temp_avg started being above 10.0 for {scandi_strct_dayspan} consecutive days
        #     by scanning the second half of the season, forwards."""
        # scandi_start_of_summer_date = df_find_in(
        #     df_timespan_data=df_2nd_half_se, 
        #     dayspan=scandi_strct_dayspan, 
        #     date_column='obs_date', 
        #     search_column='temp_avg', 
        #     all_or_mean='all', 
        #     threshold=scandi_warm_threshold, 
        #     comparison='ge'
        #     )
        

        """Find the date when the average temp_avg started being below {scandi_warm_threshold} for {scandi_avg_dayspan} consecutive days
            by scanning the first half of the season, forwards."""
        scandi_start_of_autumn_avg_based_date = df_find_in(
            df_timespan_data=df_1st_half_se,
            dayspan=scandi_avg_dayspan, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=scandi_warm_threshold, 
            comparison='le',
            offset=scandi_avg_dayspan/2 # offset forward by half the dayspan to get the middle date of the averaged result  
            )

        """Find the date when the average temp_avg started being below {scandi_cold_threshold} for {scandi_avg_dayspan} consecutive days
            by scanning the first half of the season, forwards."""
        scandi_start_of_winter_avg_based_date = df_find_in(
            df_timespan_data=df_1st_half_se,
            dayspan=scandi_avg_dayspan, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=scandi_cold_threshold, 
            comparison='le',
            offset=scandi_avg_dayspan/2 # offset forward by half the dayspan to get the middle date of the averaged result
            )

        """Find the date when the average temp_avg started being above {scandi_cold_threshold} for {scandi_avg_dayspan} consecutive days
            by scanning the second half of the season, forwards."""
        scandi_start_of_spring_avg_based_date = df_find_in(
            df_timespan_data=df_2nd_half_se, 
            dayspan=scandi_avg_dayspan, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=scandi_cold_threshold, 
            comparison='ge',
            offset=scandi_avg_dayspan/2 # offset forward by half the dayspan to get the middle date of the averaged result
            )

        """Find the date when the average temp_avg started being above {scandi_warm_threshold} for {scandi_avg_dayspan} consecutive days
            by scanning the second half of the season, forwards."""
        scandi_start_of_summer_avg_based_date = df_find_in(
            df_timespan_data=df_2nd_half_se, 
            dayspan=scandi_avg_dayspan, 
            date_column='obs_date', 
            search_column='temp_avg', 
            all_or_mean='mean', 
            threshold=scandi_warm_threshold, 
            comparison='ge',
            offset=scandi_avg_dayspan/2 # offset forward by half the dayspan to get the middle date of the averaged result
            )


        # SNOWDEPTHS

        """Find the dates when snowdepths first reach 100, 200, 300, 400, and 500 cm."""
        depths = [100, 150, 200, 250, 300, 350, 400, 450, 500, 550]
        snowdepth_date_first = {}

        for depth in depths:
            snowdepth_date_first[depth] = df_find_in(
                df_timespan_data=df_full_season,
                dayspan=1, 
                date_column='obs_date', 
                search_column='snowdepth', 
                all_or_mean='any', 
                threshold=depth,
                comparison='ge'
            )
        # depths_backwards = [550, 500, 450, 400, 350, 300, 250, 200, 150, 100]
        depths_backwards = depths[::-1]  # or list(reversed(depths))
        snowdepth_date_last = {}
        for depth in depths_backwards:
            snowdepth_date_last[depth] = df_find_in(
                df_timespan_data=df_full_season.iloc[::-1], 
                dayspan=1, 
                date_column='obs_date', 
                search_column='snowdepth', 
                all_or_mean='any', 
                threshold=depth,
                comparison='ge'
            )
        

        """Find the date when snowdepth at the end of the season is not 0.0
            by scanning the second half of the season, backwards."""
        snow_gone_date = df_find_in(
            df_timespan_data=df_2nd_half_se.iloc[::-1],
            dayspan=1, 
            date_column='obs_date', 
            search_column='snowdepth', 
            all_or_mean='any', 
            threshold=0.0, 
            comparison='gt', # this is not right, it returns the day before it goes to 0.0. using 'ne' martches NULL though…
            offset=1 # … so we add a day here to get the correct date.
            )




        """ Return the highest value of snowdepth for the season """
        max_snowdepth = df_full_season['snowdepth'].max()
        max_snowdepth = None if pd.isna(max_snowdepth) else max_snowdepth

        """ Calculate total snowfall for the season """
        total_snowfall = df_full_season['snowfall'].sum()

        """ Calculate the count of instances where snowfall was >= 10 """
        snowfall_levels = [10, 20, 30, 40, 50, 60, 70, 80]
        snowfall_level_counts = {}
        for level in snowfall_levels:
            snowfall_level_counts[level] = int((df_full_season['snowfall'] >= level).sum())


        """ Return the highest & lowest values of temp_hgh & temp_low for the season """
        columns = ['temp_avg', 'temp_hgh', 'temp_low', 'temp_amp', 'wind_speed_amp']

        # ! This is doing stats on the "traditional winter season" timespan…
        # ! November through April
        # ! Maybe not right?

        """ Set the timespan data for the season for use in full-season min/max/sum/avg calculations """
        df_trad_season = df_get_timespan_data(df=df, date_column='obs_date', year=year, start_month=12, end_month=4)

        stats = calculate_stats(df_trad_season, columns)

        next_year_abb = str(int(year) + 1)[-2:]
        season_label = f"{year}-{next_year_abb}"
        # print(season_label)

        # Store the dates in the dictionary
        seasons_data[season_label] = {
            'scandi_season_starts': {
                # 'strict' : {
                #     'aut': scandi_start_of_autumn_date,
                #     'win': scandi_start_of_winter_date,
                #     'spr': scandi_start_of_spring_date,
                #     'sum': scandi_start_of_summer_date
                # },
                'avg_based': {
                    'aut': scandi_start_of_autumn_avg_based_date,
                    'win': scandi_start_of_winter_avg_based_date,
                    'spr': scandi_start_of_spring_avg_based_date or this_season['spring_start'],
                    'sum': scandi_start_of_summer_avg_based_date or this_season['summer_start']
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
    flattened_data = dict_flatten_seasons_data(seasons_data)



    # Convert the flattened list of dictionaries to a DataFrame
    df_seasons_data = pd.DataFrame(flattened_data)



    # Write the DataFrame to a CSV file and zip it
    write_and_zip_csv(data=df_seasons_data, filename='Sukayu-Winters-Data', output_path=output_path_derived, label='DATA -', include_file_path=cl_path)



    # Write the DataFrame to an SQLite table
    df_write_to_sqlite(df_seasons_data, 'sukayu_winters_data', conn)



    # __________________________________________________________________________
    # MARK: DB Dump to Zipped CSV
    # Dump a CSV export of the whole dataset
    # Query the data
    dump_query = "SELECT * FROM obs_sukayu_daily"
    df_dump = db_query(dump_query, conn)
    if df_dump.empty:
        conn.close()
        return
    output_path_jma = os.path.join(output_path, 'jma')
    # Write the data to a tab-delimited CSV file and zip it
    write_and_zip_csv(data=df_dump, filename='sukayu_historical_obs_daily', output_path=output_path_jma, label='DB -', include_file_path=cl_path)

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



    # Close the connection
    conn.close()



    pass



if __name__ == "__main__":
    process_data()