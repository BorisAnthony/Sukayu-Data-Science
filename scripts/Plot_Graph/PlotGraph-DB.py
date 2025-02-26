import os
import sys
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import transforms
from datetime import datetime, timedelta
import subprocess
import pandas as pd
# import seaborn as sns
# from matplotlib import patheffects
# import matplotlib.colors as mcolors
# import numpy as np
# from matplotlib.font_manager import FontProperties

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
    df_get_timespan_data,
    df_find_in,
    jst,
    is_after_may_17
)
from i10n import TRANSLATIONS # Import the language settings/strings

RANGE_START = 100
RANGE_END = 600
RANGE_STEP = 50

# Language selection function
def set_language(lang='en'):
    if lang not in TRANSLATIONS:
        raise ValueError(f"Language '{lang}' not supported. Available languages: {', '.join(TRANSLATIONS.keys())}")
    return TRANSLATIONS[lang]



def load_winter_data(winter_year, db_path):
    """
    Load winter data for a specific winter year from the SQLite database.
    
    Args:
        winter_year (str): The winter season in format 'YYYY-YY'
        db_path (str): Path to the SQLite database file
    
    Returns:
        dict: Structured data for the winter season
    """
    # Connect to the database
    conn = db_connect(db_path)
    if not conn:
        print(f"Failed to connect to database to load data for {winter_year}")
        return None
    
    # Query for the specified winter year
    query = f"SELECT * FROM sukayu_winters_data WHERE season = '{winter_year}'"
    result = db_query(query, conn)
    
    # If no data found, return None
    if result is None or result.empty:
        conn.close()
        return None
    
    # Convert the DataFrame row to a dictionary for easier access
    row = result.iloc[0].to_dict()
    
    # Extract years from winter_year (e.g., "2023-24" -> 2023, 2024)
    start_year = int(winter_year.split('-')[0])
    end_year = start_year + 1
    
    # Query daily data for this winter period (September 1 of start_year to June 30 of end_year)
    daily_query = f"""
    SELECT obs_date, snowfall, snowdepth, temp_avg_7dcra 
    FROM obs_sukayu_daily 
    WHERE obs_date BETWEEN '{start_year}-09-01' AND '{end_year}-06-30' 
    ORDER BY obs_date
    """
    daily_data = db_query(daily_query, conn)
    
    # Close connection
    conn.close()
    
    # If no daily data found, create empty DataFrame
    if daily_data is None or daily_data.empty:
        daily_data = pd.DataFrame(columns=['obs_date', 'snowfall', 'snowdepth', 'temp_avg_7dcra'])
    
    # Convert obs_date to datetime
    daily_data['obs_date'] = pd.to_datetime(daily_data['obs_date'])
    
    # Build depths and milestones data structure (same as before)
    depths = {
        'max': row['snowdepths_max'],
        'first': {},
        'last': {},
        'fin': row['snowdepths_fin']
    }
    
    milestones = {}
    
    # Populate depth milestones
    for depth in range(RANGE_START, RANGE_END, RANGE_STEP):
        first_date = row[f'snowdepths_first_{depth}']
        last_date = row[f'snowdepths_last_{depth}']
        
        depths['first'][str(depth)] = first_date
        depths['last'][str(depth)] = last_date
        milestones[f'{depth}cm'] = [(first_date, last_date)]
    
    # Rest of the data structure building remains the same
    seasons = {
        'aut': row['scandi_season_starts_avg_based_aut'],
        'win': row['scandi_season_starts_avg_based_win'],
        'spr': row['scandi_season_starts_avg_based_spr'],
        'sum': row['scandi_season_starts_avg_based_sum']
    }
    
    snowfalls = {
        'fst': row['snowfalls_fst'],
        'lst': row['snowfalls_lst'],
        'fst_subs': row['snowfalls_fst_subs'],
        'lst_subs': row['snowfalls_lst_subs'],
        'total': row['snowfalls_total'],
        'days_over': {
            '10': row['snowfalls_days_over_10'],
            '20': row['snowfalls_days_over_20'],
            '30': row['snowfalls_days_over_30'],
            '40': row['snowfalls_days_over_40'],
            '50': row['snowfalls_days_over_50'],
            '60': row['snowfalls_days_over_60'],
            '70': row['snowfalls_days_over_70'],
            '80': row['snowfalls_days_over_80']
        }
    }
    
    # Return the complete data structure with daily data added
    return {
        'depths': depths,
        'milestones': milestones,
        'seasons': seasons,
        'snowfalls': snowfalls,
        'max_depth': depths['max'],
        'daily_data': daily_data  # Add daily data to the return structure
    }


def plot_winter_snow_depth(winter_year, data, lang='en'):
    
    # Get translations for selected language
    t = set_language(lang)
    
    # Then use throughout the script like:
    metadata = t['metadata']
    season_names = t['seasons']
    date_formats = t['date_formats']
    snowfall_markers = t['markers']

    if lang == 'ja':
        # Set font to support Japanese characters
        # plt.rcParams['font.family'] = 'IBM Plex Sans JP'
        # plt.rcParams['font.family'] = 'Noto Sans CJK JP'
        plt.rcParams['font.family'] = 'SF Mono Square'
        plt.rcParams['font.size'] = 14
    else:
        # Set font to Fira Code for English
        # plt.rcParams['font.family'] = 'Fira Code'
        # plt.rcParams['font.family'] = 'IBM Plex Mono'
        plt.rcParams['font.family'] = 'SF Mono Square'
        plt.rcParams['font.size'] = 12

    # Fixed dimensions for all graphs
    FIXED_WIDTH_PX = 1525  # 305 days * 5px per day
    FIXED_HEIGHT_PX = (RANGE_END + 300) # 600px for snow + 300px for seasons/labels
    DPI = 100
    
    # Convert to inches for matplotlib
    width_in = FIXED_WIDTH_PX / DPI
    height_in = FIXED_HEIGHT_PX / DPI
    
    
    # Create figure with exact size
    fig = plt.figure(figsize=(width_in, height_in), dpi=DPI)
    rndr = fig.canvas.get_renderer()
    ax = fig.add_subplot(111)
    
    # Calculate date range for this specific winter
    year = int(winter_year.split('-')[0])
    start_date = datetime(year, 9, 1)
    end_date = datetime(year + 1, 7, 1)
    
    # Set axis limits
    ax.set_xlim(start_date, end_date)
    ax.set_ylim(-300, RANGE_END)  # -200 to accommodate season blocks and labels
    
    # Set up grid
    ax.grid(True, linestyle=':', alpha=0.2)
    major_locator = mdates.MonthLocator()
    minor_locator = mdates.DayLocator()
    ax.xaxis.set_major_locator(major_locator)
    ax.xaxis.set_minor_locator(minor_locator)
    
    # Print diagnostic information
    print(f"Year: {winter_year}")
    print(f"Fixed width in pixels: {FIXED_WIDTH_PX}")
    print(f"Actual number of days: {(end_date - start_date).days}")


    # Plot daily snowfall as bars
    if 'daily_data' in data and not data['daily_data'].empty:
        # Extract snowfall data
        daily_data = data['daily_data']
        
        # Filter out NaN values
        valid_snowfall = daily_data.dropna(subset=['snowfall'])
        
        # Plot snowfall bars
        ax.bar(valid_snowfall['obs_date'], 
            valid_snowfall['snowfall'],
            width=0.75,  # 1 day width
            color='#4682B4',  # Steel blue color
            alpha=1,
            zorder=5000)  # Above the background, below the milestones


    # Draw snow depth blocks
    # depths = [500, 400, 300, 200, 100]  # Order matters for stacking
    depths = list(range(RANGE_END - RANGE_STEP, RANGE_START - RANGE_STEP, -RANGE_STEP))  # Generate depths in reverse order
    print(f"Depths: {depths}")
    colors = ['#e6e6e6'] * 10
    
    # First find the reference point for label alignment
    label_x = None
    for depth in depths:
        if data['milestones'][f'{depth}cm'][0][0] is not None:
            start_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][0], '%Y-%m-%d')
            end_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][1], '%Y-%m-%d')
            label_x = start_date + (end_date - start_date)/2
            break  # Use the first valid block's midpoint
    
    # If no blocks found, fall back to February 15th
    if label_x is None:
        label_x = datetime(year + 1, 2, 15)
    
    # Draw snow depth blocks
    block_count = 0  # Counter for blocks that actually get drawn
    for i, (depth, color) in enumerate(zip(depths, colors)):
        if data['milestones'][f'{depth}cm'][0][0] is not None:
            start_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][0], '%Y-%m-%d')
            end_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][1], '%Y-%m-%d')
            
            base_zorder = (8-i) * 10  # Higher depth blocks get lower base zorder
            
            # Draw borders and fills as before...
            trans = transforms.ScaledTranslation(0, 1/DPI, fig.dpi_scale_trans)
            line_trans = ax.transData + trans
            ax.plot([start_date, end_date], [depth, depth], color='#999999', 
                   linewidth=1, zorder=base_zorder + 1, transform=line_trans)

            trans = transforms.ScaledTranslation(-1/DPI, 0, fig.dpi_scale_trans)
            line_trans = ax.transData + trans
            ax.plot([start_date, start_date], [0, depth], color='#999999', 
                   linewidth=1, zorder=base_zorder + 1, transform=line_trans)

            trans = transforms.ScaledTranslation(1/DPI, 0, fig.dpi_scale_trans)
            line_trans = ax.transData + trans
            ax.plot([end_date, end_date], [0, depth], color='#999999', 
                   linewidth=1, zorder=base_zorder + 1, transform=line_trans)

            ax.fill_between([start_date, end_date], [depth, depth], [0, 0], 
                          color=color, alpha=1, zorder=base_zorder + 2)

            # Add start and end date labels for each block
            start_label = start_date.strftime(date_formats['m_d'])
            end_label = end_date.strftime(date_formats['m_d'])
            
            # Start date: 2 days left, 5px up, right aligned
            ax.text(start_date - timedelta(days=2), depth + 5, start_label,
                   horizontalalignment='right', verticalalignment='bottom',
                   color='#999999', size='small', zorder=base_zorder + 3)
            
            # End date: 2 days right, 5px up, left aligned
            ax.text(end_date + timedelta(days=2), depth + 5, end_label,
                   horizontalalignment='left', verticalalignment='bottom',
                   color='#999999', size='small', zorder=base_zorder + 3)

            # # DEPRECATED (for now): Label using the reference point       
            # # Label using the reference point
            # # Draw small line above label (except for first actual block)
            # if block_count > 0:  # If not the first block we've drawn
            #     # Line should be same color/width as borders
            #     # Position it 10px above the text which is at depth-10
            #     line_y = depth + 1  # text is at depth-10, so line goes 10px above that
                
            #     # Draw a short line (e.g., 10px wide centered on the label position)
            #     line_start = label_x - timedelta(days=7)  # Adjust days to control line width
            #     line_end = label_x + timedelta(days=7)
                
            #     ax.plot([line_start, line_end], [line_y, line_y], 
            #            color='#999999', linewidth=1, zorder=1000)  # Just below label zorder
            
            #     # Draw label with background
            #     ax.text(label_x, depth-10, f'{depth}cm',
            #            horizontalalignment='center', verticalalignment='top',
            #            color='#999999',
            #            bbox=dict(
            #             facecolor='#e6e6e6', 
            #             edgecolor='none', 
            #             pad=6
            #             ),
            #            zorder=990)

            # else:
            #     # First block label without background
            #     ax.text(label_x, depth-10, f'{depth}cm',
            #            horizontalalignment='center', verticalalignment='top',
            #            color='#999999',
            #            zorder=1000)
            
            block_count += 1  # Increment counter only for blocks that get drawn

    # Create season blocks along x-axis
    seasons = data['seasons']
    season_colors = {
        'aut': '#D9A18C',  # Light umber for autumn
        'win': '#94BDD1',  # Light Blue-Grey for winter
        'spr': '#AEC99C',  # Light green for spring
        'sum': '#F5DFA3'   # Dusty Yellow for summer
    }
    
    # Convert dates and create season blocks
    season_dates = []
    for season in ['aut', 'win', 'spr', 'sum']:
        if seasons[season]:
            season_dates.append((season, datetime.strptime(seasons[season], '%Y-%m-%d')))
    
    # Sort dates
    season_dates.sort(key=lambda x: x[1])
    
    # Draw complete summer bar first (spans entire width)
    year = int(winter_year.split('-')[0])
    full_start = datetime(year, 9, 1)     # September 1st
    full_end = datetime(year + 1, 7, 1)   # July 1st next year
    
    ax.fill_between([full_start, full_end], [0,0], [-50, -50],
                    color=season_colors['sum'], alpha=1.0, zorder=10)
    
    # Add summer label - only if we have a summer start date
    summer_dates = [d for d in season_dates if d[0] == 'sum']
    if summer_dates:
        
        summer_start = summer_dates[0][1]
        summer_label = summer_start + timedelta(days=2)
        summer_date = summer_start.strftime(date_formats['m_d'])

        ax.text(summer_label, -17, f"{season_names['sum']}",
                horizontalalignment='left', verticalalignment='center',
                color='#333333', zorder=12, size='small')

        ax.text(summer_label, -36, f"{summer_date}",
                horizontalalignment='left', verticalalignment='center',
                color='#333333', zorder=12, weight='bold')

        # ax.text(summer_label + timedelta(days=16), -26, f"{season_names['sum']}",
        #         horizontalalignment='left', verticalalignment='center',
        #         color='#333333', zorder=12)

    
    # Then overlay other seasons where we have dates
    if len(season_dates) >= 2:  # Need at least 2 dates to draw blocks
        for i in range(len(season_dates)-1):
            season, start = season_dates[i]
            _, end = season_dates[i+1]
            
            if season != 'sum':  # Skip summer as it's already drawn
                ax.fill_between([start, end], [0,0], [-50, -50],
                              color=season_colors[season], alpha=1.0, zorder=11)
                
                # Add season label at start of block with date
                label_x = start + timedelta(days=2)  # 10px offset
                season_text = season_names[season]
                date_text = start.strftime(date_formats['m_d'])

                ax.text(label_x, -17, f"{season_text}",
                        horizontalalignment='left', verticalalignment='center',
                        color='#333333', zorder=12, size='small')

                ax.text(label_x, -36, f"{date_text}",
                        horizontalalignment='left', verticalalignment='center',
                        color='#333333', zorder=12, weight='bold')

                # ax.text(label_x  + timedelta(days=16), -26, f"{season_text}",
                #         horizontalalignment='left', verticalalignment='center',
                #         color='#333333', zorder=12)

    # draw the top border for the summer block
    ax.plot([full_start, full_end], [0, 0], color='black', linewidth=0.5, zorder=100)


    # First/Last Snowfall Dates ------------------------------------------------
    # Draw snowfall date markers
    snowfalls = data['snowfalls']
    markers_y_top = -52     # Start at same height as season blocks
    markers_y_bot = -110    # Extend 100px down
    markers_subs_y_bot = -160    # Extend 100px down
    label_offset = -10      # Additional offset for labels
    label_exp_offset = -20  # Additional offset for expanded labels
    
    if snowfalls['fst'] is not None:
        first_snow = datetime.strptime(snowfalls['fst'], '%Y-%m-%d')
        ax.plot([first_snow, first_snow], [markers_y_top, markers_y_bot], 
                color='#2778BE', linewidth=2, zorder=-300, 
                solid_capstyle='butt')
        ax.text(first_snow, markers_y_bot + label_offset, 
                first_snow.strftime(date_formats['m_d']),
                horizontalalignment='right', verticalalignment='top', zorder=-300,
                color='#2778BE', weight='bold')
        ax.text(first_snow, markers_y_bot + label_offset + label_exp_offset, 
                snowfall_markers['fst'],
                horizontalalignment='right', verticalalignment='top', zorder=-300,
                color='#2778BE', size='small')

    if snowfalls['fst_subs'] is not None:
        first_subs = datetime.strptime(snowfalls['fst_subs'], '%Y-%m-%d')
        ax.plot([first_subs, first_subs], [markers_y_top, markers_subs_y_bot], 
                color='#325C81', linewidth=2, zorder=-300, 
                solid_capstyle='butt')
        ax.text(first_subs, markers_subs_y_bot + label_offset, 
                first_subs.strftime(date_formats['m_d']),
                horizontalalignment='right', verticalalignment='top', zorder=-300,
                color='#325C81', weight='bold')
        ax.text(first_subs, markers_subs_y_bot + label_offset + label_exp_offset, 
                snowfall_markers['fst_subs'],
                horizontalalignment='right', verticalalignment='top', zorder=-300,
                color='#325C81', size='small')

    if snowfalls['lst_subs'] is not None:
        last_subs = datetime.strptime(snowfalls['lst_subs'], '%Y-%m-%d')
        ax.plot([last_subs, last_subs], [markers_y_top, markers_subs_y_bot], 
                color='#325C81', linewidth=2, zorder=-300, 
                solid_capstyle='butt')
        ax.text(last_subs, markers_subs_y_bot + label_offset, 
                last_subs.strftime(date_formats['m_d']),
                # horizontalalignment='left',
                horizontalalignment='right' if is_after_may_17(last_subs) else 'left',
                verticalalignment='top', zorder=-300,
                color='#325C81', weight='bold')
        ax.text(last_subs, markers_subs_y_bot + label_offset + label_exp_offset, 
                snowfall_markers['lst_subs'],
                # horizontalalignment='left', 
                horizontalalignment='right' if is_after_may_17(last_subs) else 'left',
                verticalalignment='top', zorder=-300,
                color='#325C81', size='small')

    if snowfalls['lst'] is not None:
        last_snow = datetime.strptime(snowfalls['lst'], '%Y-%m-%d')
        ax.plot([last_snow, last_snow], [markers_y_top, markers_y_bot], 
                color='#2778BE', linewidth=2, zorder=-300, 
                solid_capstyle='butt')
        ax.text(last_snow, markers_y_bot + label_offset, 
                last_snow.strftime(date_formats['m_d']),
                horizontalalignment='left', verticalalignment='top', zorder=-300,
                color='#2778BE', weight='bold')
        ax.text(last_snow, markers_y_bot + label_offset + label_exp_offset, 
                snowfall_markers['lst'],
                horizontalalignment='left', verticalalignment='top', zorder=-300,
                color='#2778BE', size='small')



    # Snowdepth Fin ------------------------------------------------------------
    # Draw final snow date marker
    if data['depths']['fin'] is not None:
        final_snow = datetime.strptime(data['depths']['fin'], '%Y-%m-%d')
        markers_y_bot = 0      # Start at y=0
        markers_y_top = 62    # Extend 100px up
        
        # Draw dotted line
        ax.plot([final_snow, final_snow], [markers_y_bot, markers_y_top], 
                color='black', linewidth=2, zorder=300, 
                linestyle=':', solid_capstyle='butt')
        
        # Add label
        ax.text(final_snow + timedelta(days=2), markers_y_top - 14, 
                final_snow.strftime(date_formats['m_d']),
                horizontalalignment='left', verticalalignment='bottom',
                color='black', weight='bold')
        ax.text(final_snow + timedelta(days=2), markers_y_top - 34, 
                snowfall_markers['fin'],
                horizontalalignment='left', verticalalignment='bottom',
                color='black', size='small')



    # X Axis Spine & Ticks -----------------------------------------------------
    # ax.xaxis.set_major_formatter(mdates.DateFormatter(date_formats['m']))
    # # this lowercases the labels. comment out to disable
    # ax.set_xticklabels([label.get_text().lower() for label in ax.get_xticklabels()])

    # Move the bottom spine to under the Seasons
    ax.spines['bottom'].set_position(('axes', 0.275))
    ax.spines['bottom'].set_zorder(1000)  # Move to front

    # For the x-axis ticks
    ax.xaxis.set_major_formatter(mdates.DateFormatter(date_formats['m']))
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=15))
    # Get the labels and convert to lowercase
    ticks = ax.get_xticks()
    ax.set_xticks(ticks)
    ax.set_xticklabels([ax.xaxis.get_major_formatter().format_data(tick).lower() for tick in ticks])
    ax.xaxis.grid(False)

    # xaxis tick labels 
    # centered on the month
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=15))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    
    ax.tick_params(axis='x', which='major', length=0, pad=10, labelcolor='#999999', labelsize='small')
    ax.tick_params(axis='x', which='minor', length=10, color='#999999')

    # Add bbox styling to the tick labels
    for label in ax.get_xticklabels():
        bbox = dict(
            facecolor='white',      # or any color you prefer
            edgecolor='none',       # removes the border
            alpha=1,              # adjust transparency as needed
            pad=4                   # adjust padding as needed
        )
        label.set(bbox=bbox)           # Using set() method instead
    
    # print([tick.get_text() for tick in ax.get_xticklabels()])
    # ax.xaxis.set_tick_params(which='major', labelbottom=True)

    # Draw Jan 1st tick mark
    jan_first = datetime(year + 1, 1, 1)  # January 1st of the winter year
    ax.plot([jan_first, jan_first], [0, -51], 
            color='white', linewidth=2, zorder=300, alpha=0.3, 
            solid_capstyle='butt')  # Use butt capstyle for sharp ends
    ax.plot([jan_first, jan_first], [-51, -68], 
            color='black', linewidth=2, zorder=300, alpha=0.6,
            solid_capstyle='butt')  # Use butt capstyle for sharp ends
    


    # Other Axis Options -------------------------------------------------------
    # Remove the top and left spines
    ax.spines['top'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    # Set Y Axis Ticket, on right side
    # Move y-axis ticks and labels to the right side
    ax.yaxis.tick_right()
    ax.yaxis.set_label_position("left")
    ax.tick_params(axis='y', colors='#cccccc')
    ax.yaxis.label.set_color('#cccccc')
    # Set Y axis spine and ticks to end at 0
    ax.spines['right'].set_bounds(0, RANGE_END)
    ax.spines['right'].set_visible(True)
    ax.spines['right'].set_color('#cccccc')
    ax.yaxis.set_ticks_position('right')
    ax.yaxis.set_ticks(range(0, RANGE_END + 1, RANGE_STEP * 2))
    # Set Y axis grid lines color
    # ax.yaxis.grid(True, color='#cccccc', linestyle=':', linewidth=0.5)
    ax.yaxis.grid(True, color='#333333', zorder=0)
    # Remove y-axis ticks
    # ax.yaxis.set_ticks([])
    


    # TITLE & METADATA ---------------------------------------------------------
    # Set up the title
    title = f"{season_names['win']} {winter_year}"
    ax.set_title(title, fontsize=16, fontweight='bold', loc='left', pad=20)
    # ax.text(0, 1, 
    #         f'{metadata['total_snowfall']}: {int(data["snowfalls"]["total"]):4d} cm\n{metadata['max_snow_depth']}: {int(data["max_depth"]):4d} cm',
    #         transform=ax.transAxes, verticalalignment='top', fontsize=14)

    total_snow = int(data["snowfalls"]["total"]) if data["snowfalls"]["total"] is not None else 0
    max_depth = int(data["max_depth"]) if data["max_depth"] is not None else 0
    
    ax.text(0, 1, 
            f'{metadata["total_snowfall"]}: {total_snow:4d} cm\n{metadata["max_snow_depth"]}: {max_depth:4d} cm',
            transform=ax.transAxes, verticalalignment='top', fontsize=14)

    print(f"Completed plotting {winter_year}")
    return fig

# # Example usage
# winter_year = "2023-24"
# data = load_winter_data('../outputs/derived/Sukayu-Winters-Data.json', winter_year)
# fig = plot_winter_snow_depth(winter_year, data, lang='ja')
# plt.show()


# PROCESS ALL WINTERS ----------------------------------------------------------

def process_all_winters(output_dir, db_path):
    """
    Process all winter years from the SQLite database and generate visualizations.
    
    Args:
        output_dir (str): Directory to save the output images
        db_path (str): Path to the SQLite database file
    """
    # Connect to the database
    conn = db_connect(db_path)
    if not conn:
        print("Failed to connect to database")
        return
    
    # Query all winter years ordered by season
    query = "SELECT season FROM sukayu_winters_data ORDER BY season"
    results = db_query(query, conn)
    
    # Close connection
    conn.close()
    
    if results is None or results.empty:
        print("No winter seasons found in the database")
        return
    
    # Extract winter years from results
    winter_years = results['season'].tolist()
    
    # Process each winter year
    for winter_year in winter_years:
        # Load data for this winter year
        data = load_winter_data(winter_year, db_path)
        
        if data is None:
            print(f"No data found for winter year: {winter_year}, skipping...")
            continue
        
        # Generate language versions
        for lang in ['en']:  # Can be expanded to ['en', 'ja'] as needed
            # Create the figure
            fig = plot_winter_snow_depth(winter_year, data, lang)
            
            # Create output filename
            output_file = f"{output_dir}/{lang}/{winter_year}.png"
            
            # Save with high DPI and tight layout
            fig.savefig(output_file, 
                       dpi=100,
                       bbox_inches='tight',
                       pad_inches=0.2)
            
            # Clear the figure to free memory
            plt.close(fig)
            
            print(f"Generated: {output_file}")

# Run the batch process
output_dir = "../outputs/figures"
output_dir_en = "../outputs/figures/en"
output_dir_ja = "../outputs/figures/ja"
# json_file = "../outputs/derived/Sukayu-Winters-Data.json"
db_path = "../database/dist/sukayu_historical_obs_daily_everything.sqlite" 

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
os.makedirs(output_dir_en, exist_ok=True)
os.makedirs(output_dir_ja, exist_ok=True)

process_all_winters(output_dir, db_path)

def create_video_from_pngs(input_pattern, output_file, frame_duration=1):
    """
    Create video from PNG files.
    input_pattern: Path pattern for input files, e.g., "../outputs/figures/*.en.png"
    output_file: Path for output video file, e.g., "../outputs/figures/animation.mp4"
    frame_duration: How long each frame should show (in seconds)
    """
    ffmpeg_cmd = [
        'ffmpeg',
        '-y',  # Overwrite output file if it exists
        '-framerate', f'1/{frame_duration}',  # One frame every 2 seconds
        '-pattern_type', 'glob',
        '-i', input_pattern,
        '-c:v', 'libx264',  # Use H.264 codec
        '-pix_fmt', 'yuv420p',  # Required for compatibility
        '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2,fps=30',  # Output video framerate
        output_file
    ]
    
    try:
        subprocess.run(ffmpeg_cmd, check=True)
        print(f"Video created successfully: {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error creating video: {e}")

# Create videos for both languages
# create_video_from_pngs("../outputs/figures/en/*.png", "../outputs/figures/en/winters.mp4")
# create_video_from_pngs("../outputs/figures/ja/*.png", "../outputs/figures/ja/winters.mp4")