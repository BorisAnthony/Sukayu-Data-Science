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
import numpy as np
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

# Temperature visualization constants
TEMP_MAX = 15  # Maximum temperature shown (°C)
TEMP_MIN = -15  # Minimum temperature shown (°C)
TEMP_RANGE = TEMP_MAX - TEMP_MIN
SEASON_BLOCK_HEIGHT = 180  # Increased height for season blocks to accommodate temperature chart

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


def plot_winter_seasons_only(winter_year, data, lang='en'):
    """
    Plot only the season blocks for a winter year with consistent dimensions.
    """
    # Get translations
    t = set_language(lang)
    season_names = t['seasons']
    date_formats = t['date_formats']
    
    # Font settings
    plt.rcParams['font.family'] = 'SF Mono Square'
    plt.rcParams['font.size'] = 12 if lang == 'en' else 14

    # Base parameters in pixels
    GRAPH_WIDTH_PX = 1525  # Fixed graph width in pixels
    SEASON_BLOCK_HEIGHT_PX = 40  # Height of season blocks in pixels
    VERTICAL_PADDING_PX = 20  # Padding above and below blocks
    TOTAL_HEIGHT_PX = SEASON_BLOCK_HEIGHT_PX + VERTICAL_PADDING_PX * 2
    
    # Matplotlib requires inches and DPI
    DPI = 100  # Fixed DPI
    # Convert pixels to inches for matplotlib
    width_in = GRAPH_WIDTH_PX / DPI
    height_in = TOTAL_HEIGHT_PX / DPI
    
    # Create figure with exact size
    fig = plt.figure(figsize=(width_in, height_in), dpi=DPI)
    ax = fig.add_axes([0, 0, 1, 1])  # Uses full figure area
    
    # Date range - must match full graph
    year = int(winter_year.split('-')[0])
    start_date = datetime(year, 9, 1)
    end_date = datetime(year + 1, 7, 1)
    ax.set_xlim(start_date, end_date)
    
    # Calculate coordinate space
    # We'll center the blocks vertically with equal padding above and below
    block_top = VERTICAL_PADDING_PX
    block_bottom = -SEASON_BLOCK_HEIGHT_PX
    ax.set_ylim(block_bottom - VERTICAL_PADDING_PX, block_top + VERTICAL_PADDING_PX)
    
    # Remove all spines
    for spine in ax.spines.values():
        spine.set_visible(False)
    
    # Season colors
    season_colors = {
        'aut': '#D9A18C',  # Autumn
        'win': '#94BDD1',  # Winter
        'spr': '#AEC99C',  # Spring
        'sum': '#F5DFA3'   # Summer
    }
    
    # Convert dates and create season blocks
    season_dates = []
    for season in ['aut', 'win', 'spr', 'sum']:
        if seasons := data['seasons'].get(season):
            season_dates.append((season, datetime.strptime(seasons, '%Y-%m-%d')))
    season_dates.sort(key=lambda x: x[1])
    
    # Draw summer bar (spans entire width)
    ax.fill_between([start_date, end_date], [block_top, block_top], 
                    [block_bottom, block_bottom],
                    color=season_colors['sum'], alpha=1.0, zorder=10)
    
    # Text positions - calculate relative to block
    text_y_season = block_bottom + SEASON_BLOCK_HEIGHT_PX * 0.65  # 65% from bottom
    text_y_date = block_bottom + SEASON_BLOCK_HEIGHT_PX * 0.25    # 25% from bottom
    
    # Add summer label if we have a date
    summer_dates = [d for d in season_dates if d[0] == 'sum']
    if summer_dates:
        summer_start = summer_dates[0][1]
        summer_label = summer_start + timedelta(days=2)
        summer_date = summer_start.strftime(date_formats['m_d'])

        ax.text(summer_label, text_y_season, f"{season_names['sum']}",
                horizontalalignment='left', verticalalignment='center',
                color='#333333', zorder=12, size='small')

        ax.text(summer_label, text_y_date, f"{summer_date}",
                horizontalalignment='left', verticalalignment='center',
                color='#333333', zorder=12, weight='bold')
    
    # Overlay other seasons
    if len(season_dates) >= 2:
        for i in range(len(season_dates)-1):
            season, start = season_dates[i]
            _, end = season_dates[i+1]
            
            if season != 'sum':
                ax.fill_between([start, end], [block_top, block_top], 
                                [block_bottom, block_bottom],
                                color=season_colors[season], alpha=1.0, zorder=11)
                
                label_x = start + timedelta(days=2)
                season_text = season_names[season]
                date_text = start.strftime(date_formats['m_d'])

                ax.text(label_x, text_y_season, f"{season_text}",
                        horizontalalignment='left', verticalalignment='center',
                        color='#333333', zorder=12, size='small')

                ax.text(label_x, text_y_date, f"{date_text}",
                        horizontalalignment='left', verticalalignment='center',
                        color='#333333', zorder=12, weight='bold')

    # Draw the top border
    ax.plot([start_date, end_date], [block_top, block_top], 
            color='black', linewidth=0.5, zorder=100)
    
    # Draw Jan 1st tick mark
    jan_first = datetime(year + 1, 1, 1)
    ax.plot([jan_first, jan_first], [block_top, block_bottom], 
            color='white', linewidth=2, zorder=300, alpha=0.3, 
            solid_capstyle='butt')
    
    # Remove all ticks and labels
    ax.set_xticks([])
    ax.set_yticks([])
    
    return fig


def plot_winter_snow_depth(winter_year, data, lang='en'):
    """
    Plot winter snow depth data with season blocks and temperature data.
    
    Args:
        winter_year (str): The winter season in format 'YYYY-YY'
        data (dict): The data structure containing winter season information
        lang (str): Language code ('en' or 'ja')
    
    Returns:
        matplotlib.figure.Figure: The created figure
    """
    # Get translations for selected language
    t = set_language(lang)
    
    # Then use throughout the script like:
    metadata = t['metadata']
    season_names = t['seasons']
    date_formats = t['date_formats']
    snowfall_markers = t['markers']

    # Font settings
    plt.rcParams['font.family'] = 'SF Mono Square'
    plt.rcParams['font.size'] = 12 if lang == 'en' else 14

    # Base parameters in pixels with clearer organization
    GRAPH_WIDTH_PX = 1525                    # Fixed graph width in pixels
    SNOW_DEPTH_AREA_PX = RANGE_END           # Area for snow depth visualization (600px)
    SEASON_BLOCK_HEIGHT_PX = 200             # Increased height for season blocks
    SNOW_MARKERS_HEIGHT_PX = 120             # Height for snowfall markers
    
    # Temperature visualization parameters
    TEMP_MAX = 15                            # Maximum temperature (°C)
    TEMP_MIN = -15                           # Minimum temperature (°C)
    TEMP_RANGE = TEMP_MAX - TEMP_MIN         # Temperature range for scaling
    
    # Calculate total height
    TOTAL_HEIGHT_PX = SNOW_DEPTH_AREA_PX + SEASON_BLOCK_HEIGHT_PX + SNOW_MARKERS_HEIGHT_PX
    
    # Matplotlib requires inches and DPI
    DPI = 100
    width_in = GRAPH_WIDTH_PX / DPI
    height_in = TOTAL_HEIGHT_PX / DPI
    
    # Create figure with exact size
    fig = plt.figure(figsize=(width_in, height_in), dpi=DPI)
    # Eliminate all internal margins
    # fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    # Use moderate margins that still allow elements to be visible
    # fig.subplots_adjust(left=0.025, right=0.95, top=0.9, bottom=0.1)
    rndr = fig.canvas.get_renderer()
    ax = fig.add_subplot(111)
    
    # Calculate date range
    year = int(winter_year.split('-')[0])
    start_date = datetime(year, 9, 1)
    end_date = datetime(year + 1, 7, 1)
    
    # Calculate coordinate ranges for different sections
    snow_top = SNOW_DEPTH_AREA_PX
    season_top = 0
    season_bottom = -SEASON_BLOCK_HEIGHT_PX
    marker_top = season_bottom
    marker_bottom = marker_top - SNOW_MARKERS_HEIGHT_PX
    
    # Set axis limits
    ax.set_xlim(start_date, end_date)
    ax.set_ylim(marker_bottom, snow_top)
    
    # Set up grid
    ax.grid(True, linestyle=':', alpha=0.2)
    major_locator = mdates.MonthLocator()
    minor_locator = mdates.DayLocator()
    ax.xaxis.set_major_locator(major_locator)
    ax.xaxis.set_minor_locator(minor_locator)
    
    # Print diagnostic information
    print(f"Year: {winter_year}")
    print(f"Fixed width in pixels: {GRAPH_WIDTH_PX}")
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
    depths = list(range(RANGE_END - RANGE_STEP, RANGE_START - RANGE_STEP, -RANGE_STEP))  # Generate depths in reverse order
    print(f"Depths: {depths}")
    colors = ['#e6e6e6'] * 10
    
    # First find the reference point for label alignment
    label_x = None
    for depth in depths:
        if data['milestones'][f'{depth}cm'][0][0] is not None:
            start_date_str = data['milestones'][f'{depth}cm'][0][0]
            end_date_str = data['milestones'][f'{depth}cm'][0][1]
            start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d')
            label_x = start_date_obj + (end_date_obj - start_date_obj)/2
            break  # Use the first valid block's midpoint
    
    # If no blocks found, fall back to February 15th
    if label_x is None:
        label_x = datetime(year + 1, 2, 15)
    
    # Draw snow depth blocks
    block_count = 0  # Counter for blocks that actually get drawn
    for i, (depth, color) in enumerate(zip(depths, colors)):
        if data['milestones'][f'{depth}cm'][0][0] is not None:
            start_date_str = data['milestones'][f'{depth}cm'][0][0]
            end_date_str = data['milestones'][f'{depth}cm'][0][1]
            start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d')
            
            base_zorder = (8-i) * 10  # Higher depth blocks get lower base zorder
            
            # Draw borders and fills as before...
            trans = transforms.ScaledTranslation(0, 1/DPI, fig.dpi_scale_trans)
            line_trans = ax.transData + trans
            ax.plot([start_date_obj, end_date_obj], [depth, depth], color='#999999', 
                   linewidth=1, zorder=base_zorder + 1, transform=line_trans)

            trans = transforms.ScaledTranslation(-1/DPI, 0, fig.dpi_scale_trans)
            line_trans = ax.transData + trans
            ax.plot([start_date_obj, start_date_obj], [0, depth], color='#999999', 
                   linewidth=1, zorder=base_zorder + 1, transform=line_trans)

            trans = transforms.ScaledTranslation(1/DPI, 0, fig.dpi_scale_trans)
            line_trans = ax.transData + trans
            ax.plot([end_date_obj, end_date_obj], [0, depth], color='#999999', 
                   linewidth=1, zorder=base_zorder + 1, transform=line_trans)

            ax.fill_between([start_date_obj, end_date_obj], [depth, depth], [0, 0], 
                          color=color, alpha=1, zorder=base_zorder + 2)

            # Add start and end date labels for each block
            start_label = start_date_obj.strftime(date_formats['m_d'])
            end_label = end_date_obj.strftime(date_formats['m_d'])
            
            # Start date: 2 days left, 5px up, right aligned
            ax.text(start_date_obj - timedelta(days=2), depth + 5, start_label,
                   horizontalalignment='right', verticalalignment='bottom',
                   color='#999999', size='small', zorder=base_zorder + 3)
            
            # End date: 2 days right, 5px up, left aligned
            ax.text(end_date_obj + timedelta(days=2), depth + 5, end_label,
                   horizontalalignment='left', verticalalignment='bottom',
                   color='#999999', size='small', zorder=base_zorder + 3)
            
            block_count += 1  # Increment counter only for blocks that get drawn

    # SEASON BLOCKS WITH TEMPERATURE OVERLAY
    seasons = data['seasons']
    season_colors = {
        'aut': '#D9A18C',  # Light umber for autumn
        'win': '#94BDD1',  # Light Blue-Grey for winter
        'spr': '#AEC99C',  # Light green for spring
        'sum': '#F5DFA3'   # Dusty Yellow for summer
    }
    
    # Convert dates for season blocks
    season_dates = []
    for season in ['aut', 'win', 'spr', 'sum']:
        if seasons[season]:
            season_dates.append((season, datetime.strptime(seasons[season], '%Y-%m-%d')))
    season_dates.sort(key=lambda x: x[1])
    
    # Draw summer bar first (spans entire width)
    full_start = datetime(year, 9, 1)
    full_end = datetime(year + 1, 7, 1)
    
    ax.fill_between([full_start, full_end], [season_top, season_top], [season_bottom, season_bottom],
                    color=season_colors['sum'], alpha=1.0, zorder=10)
    
    # Add summer label - only if we have a summer start date
    summer_dates = [d for d in season_dates if d[0] == 'sum']
    if summer_dates:
        summer_start = summer_dates[0][1]
        summer_label = summer_start + timedelta(days=2)
        summer_date = summer_start.strftime(date_formats['m_d'])
        
        # Position labels higher in the taller season blocks
        text_y_season = season_bottom + SEASON_BLOCK_HEIGHT_PX * 0.2
        text_y_date = season_bottom + SEASON_BLOCK_HEIGHT_PX * 0.1

        ax.text(summer_label, text_y_season, f"{season_names['sum']}",
                horizontalalignment='left', verticalalignment='center',
                color='#333333', zorder=12, size='small')

        ax.text(summer_label, text_y_date, f"{summer_date}",
                horizontalalignment='left', verticalalignment='center',
                color='#333333', zorder=12, weight='bold')
    
    # Then overlay other seasons where we have dates
    if len(season_dates) >= 2:  # Need at least 2 dates to draw blocks
        for i in range(len(season_dates)-1):
            season, start = season_dates[i]
            _, end = season_dates[i+1]
            
            if season != 'sum':  # Skip summer as it's already drawn
                ax.fill_between([start, end], [season_top, season_top], [season_bottom, season_bottom],
                              color=season_colors[season], alpha=1.0, zorder=11)
                
                # Add season label at start of block with date
                label_x = start + timedelta(days=2)  # 10px offset
                season_text = season_names[season]
                date_text = start.strftime(date_formats['m_d'])
                
                # Position labels higher in the taller season blocks
                # text_y_season = season_bottom + SEASON_BLOCK_HEIGHT_PX * 0.85
                # text_y_date = season_bottom + SEASON_BLOCK_HEIGHT_PX * 0.75

                ax.text(label_x, text_y_season, f"{season_text}",
                        horizontalalignment='left', verticalalignment='center',
                        color='#333333', zorder=12, size='small')

                ax.text(label_x, text_y_date, f"{date_text}",
                        horizontalalignment='left', verticalalignment='center',
                        color='#333333', zorder=12, weight='bold')

    # Draw the top border for the summer block
    ax.plot([full_start, full_end], [season_top, season_top], color='black', linewidth=0.5, zorder=100)
    
    # TEMPERATURE OVERLAY ON SEASON BLOCKS
    if 'daily_data' in data and not data['daily_data'].empty:
        valid_temp = data['daily_data'].dropna(subset=['temp_avg_7dcra'])
        
        if not valid_temp.empty:
            # Updated temperature range to -20°C to +20°C
            TEMP_MAX = 20                            # Maximum temperature (°C)
            TEMP_MIN = -20                           # Minimum temperature (°C)
            TEMP_RANGE = TEMP_MAX - TEMP_MIN         # Temperature range for scaling
            
            # Function to map temperature values to the season block area
            def temp_to_y(temp):
                # Normalize temperature to 0-1 range within TEMP_MIN to TEMP_MAX
                norm_temp = (temp - TEMP_MIN) / TEMP_RANGE
                # Map to season block area (bottom to top)
                return season_bottom + norm_temp * SEASON_BLOCK_HEIGHT_PX
            
            # Extract data for easier processing
            dates = valid_temp['obs_date'].tolist()
            temps = valid_temp['temp_avg_7dcra'].tolist()
            y_values = [temp_to_y(temp) for temp in temps]
            
            # Identify where temperature crosses zero
            # and create segments with appropriate colors
            above_segments = []  # Will hold red segments
            below_segments = []  # Will hold blue segments
            
            # Process the data to find segments and zero-crossings
            for i in range(len(dates) - 1):
                date1, date2 = dates[i], dates[i+1]
                temp1, temp2 = temps[i], temps[i+1]
                y1, y2 = y_values[i], y_values[i+1]
                
                # Check if we cross zero between these points
                if (temp1 > 0 and temp2 <= 0) or (temp1 <= 0 and temp2 > 0):
                    # Crossing occurs - calculate exact crossing point
                    if temp2 != temp1:  # Avoid division by zero
                        frac = -temp1 / (temp2 - temp1)
                        delta = (date2 - date1).total_seconds()
                        crossing_date = date1 + timedelta(seconds=delta * frac)
                        crossing_y = y1 + (y2 - y1) * frac
                        
                        # Create segments to and from the crossing point
                        if temp1 > 0:
                            above_segments.append((date1, crossing_date, y1, crossing_y))
                            below_segments.append((crossing_date, date2, crossing_y, y2))
                        else:
                            below_segments.append((date1, crossing_date, y1, crossing_y))
                            above_segments.append((crossing_date, date2, crossing_y, y2))
                else:
                    # No crossing - add the whole segment to the appropriate list
                    if temp1 > 0 and temp2 > 0:
                        above_segments.append((date1, date2, y1, y2))
                    else:
                        below_segments.append((date1, date2, y1, y2))
            
            # Plot each segment with the appropriate color
            for d1, d2, y1, y2 in above_segments:
                ax.plot([d1, d2], [y1, y2], color='#E74C3C', linewidth=1.5, alpha=1, zorder=20, solid_joinstyle='round')
                
            for d1, d2, y1, y2 in below_segments:
                ax.plot([d1, d2], [y1, y2], color='#2778BE', linewidth=1.5, alpha=1, zorder=20, solid_joinstyle='round')
            
            
            
            # Add 0°C reference line exactly in the middle of the season blocks
            zero_y = temp_to_y(0)
            ten_y = temp_to_y(10)
            ten_m_y = temp_to_y(-10)
            ax.axhline(y=zero_y, color='#000000', linestyle='-', linewidth=0.5, alpha=0.25, zorder=19)
            ax.axhline(y=ten_y, color='#000000', linestyle='-', linewidth=0.5, alpha=0.25, zorder=19)
            ax.axhline(y=ten_m_y, color='#000000', linestyle='-', linewidth=0.5, alpha=0.25, zorder=19)
            
            # Create a separate coordinate system just for temperature
            ax_temp = plt.axes([0.1, 0, 0.8, 1], frameon=False)
            ax_temp.patch.set_visible(False)
            ax_temp.xaxis.set_visible(False)
            
            # Position the y-axis at the right edge
            ax_temp.spines['right'].set_position(('outward', 5))
            
            # Only show the right spine
            ax_temp.spines['left'].set_visible(False)
            ax_temp.spines['top'].set_visible(False)
            ax_temp.spines['bottom'].set_visible(False)
            
            # Set the temperature axis range to exactly correspond to the temperature values
            ax_temp.set_ylim(TEMP_MIN, TEMP_MAX)
            
            # Calculate the y-positions in the new axes corresponding to the season block area
            # Convert from data coordinates to normalized figure coordinates
            season_bottom_norm = ax.transData.transform((0, season_bottom))[1] / fig.get_figheight() / DPI
            season_top_norm = ax.transData.transform((0, season_top))[1] / fig.get_figheight() / DPI
            
            # Set the position and height of the temperature axis to match the season blocks
            ax_temp.set_position([
                0.8756,  # x position (right side)
                season_bottom_norm,  # y position (bottom of season blocks)
                0.02,  # width
                season_top_norm - season_bottom_norm  # height matching season blocks
            ])

            # Set the ticks
            ax_temp.set_yticks([-10, 0, 10])

            # Style just the tick marks to be grey
            ax_temp.tick_params(axis='y', colors='#999999', length=4, direction='out')

            # Remove existing labels first
            ax_temp.set_yticklabels([])

            # Manually add text objects at the correct positions
            for i, (pos, label, color) in enumerate([
                (-10, "-10 °C", '#2778BE'),  # Blue
                (0, "  0 °C", '#999999'),    # Grey
                (10, "+10 °C", '#E74C3C')    # Red
            ]):
                ax_temp.text(
                    x=1.65,  # This is the default position for right-aligned tick labels
                    y=pos - 0.2,
                    s=label,
                    transform=ax_temp.get_yaxis_transform(),
                    va='center',
                    ha='left',
                    color=color,
                    fontsize=12
                )

            # Set axis position
            ax_temp.yaxis.tick_right()
            ax_temp.yaxis.set_ticks_position('right')
            ax_temp.yaxis.set_label_position("left")
            
            # Add temperature label
            # ax_temp.set_ylabel('Temperature (°C)', color='#E74C3C', fontsize=11)
            # ax_temp.yaxis.set_label_position('right')

    # First/Last Snowfall Dates ------------------------------------------------
    # Draw snowfall date markers - with adjusted positions for the taller season blocks
    snowfalls = data['snowfalls']
    markers_y_top = marker_top      # Start at same height as season blocks
    markers_y_bot = marker_top - 60      # First marker end position
    markers_subs_y_bot = marker_top - 110  # Second marker end position
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
                horizontalalignment='right' if is_after_may_17(last_subs) else 'left',
                verticalalignment='top', zorder=-300,
                color='#325C81', weight='bold')
        ax.text(last_subs, markers_subs_y_bot + label_offset + label_exp_offset, 
                snowfall_markers['lst_subs'],
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
        fin_markers_y_bot = 0      # Start at y=0
        fin_markers_y_top = 62    # Extend up
        
        # Draw dotted line
        ax.plot([final_snow, final_snow], [fin_markers_y_bot, fin_markers_y_top], 
                color='black', linewidth=2, zorder=300, 
                linestyle=':', solid_capstyle='butt')
        
        # Add label
        ax.text(final_snow + timedelta(days=2), fin_markers_y_top - 14, 
                final_snow.strftime(date_formats['m_d']),
                horizontalalignment='left', verticalalignment='bottom',
                color='black', weight='bold')
        ax.text(final_snow + timedelta(days=2), fin_markers_y_top - 34, 
                snowfall_markers['fin'],
                horizontalalignment='left', verticalalignment='bottom',
                color='black', size='small')

    # X Axis Spine & Ticks -----------------------------------------------------
    # Move the bottom spine to under the Seasons - calculated position
    ax.spines['bottom'].set_position(('data', season_bottom))
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
        label.set(bbox=bbox)
    
    # # Draw Jan 1st tick mark
    # jan_first = datetime(year + 1, 1, 1)  # January 1st of the winter year
    # ax.plot([jan_first, jan_first], [0, -51], 
    #         color='white', linewidth=2, zorder=300, alpha=0.3, 
    #         solid_capstyle='butt')  # Use butt capstyle for sharp ends
    # ax.plot([jan_first, jan_first], [-51, -68], 
    #         color='black', linewidth=2, zorder=300, alpha=0.6,
    #         solid_capstyle='butt')  # Use butt capstyle for sharp ends



    # Draw Jan 1st tick mark
    jan_first = datetime(year + 1, 1, 1)  # January 1st of the winter year
    ax.plot([jan_first, jan_first], [season_top, season_bottom], 
            color='white', linewidth=2, zorder=300, alpha=0.3, 
            solid_capstyle='butt')
    ax.plot([jan_first, jan_first], [season_bottom, season_bottom - 18], 
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
    ax.yaxis.grid(True, color='#333333', zorder=0)

    # TITLE & METADATA ---------------------------------------------------------
    # Set up the title
    title = f"{season_names['win']} {winter_year}"
    ax.set_title(title, fontsize=16, fontweight='bold', loc='left', pad=20)

    total_snow = int(data["snowfalls"]["total"]) if data["snowfalls"]["total"] is not None else 0
    max_depth = int(data["max_depth"]) if data["max_depth"] is not None else 0
    
    metadata_text = f'{metadata["total_snowfall"]}: {total_snow:4d} cm\n{metadata["max_snow_depth"]}: {max_depth:4d} cm'
    
    # # Add temperature metadata if available
    # if 'daily_data' in data and not data['daily_data'].empty and 'temp_avg_7dcra' in data['daily_data'].columns:
    #     temp_data = data['daily_data']['temp_avg_7dcra'].dropna()
    #     if not temp_data.empty:
    #         temp_min = temp_data.min()
    #         temp_max = temp_data.max()
    #         temp_avg = temp_data.mean()
            
    #         metadata_text += f'\n{metadata.get("temp_avg", "Avg Temp")}: {temp_avg:.1f}°C'
    #         metadata_text += f'\n{metadata.get("temp_min", "Min Temp")}: {temp_min:.1f}°C'
    #         metadata_text += f'\n{metadata.get("temp_max", "Max Temp")}: {temp_max:.1f}°C'
    
    ax.text(0, 1, metadata_text,
            transform=ax.transAxes, verticalalignment='top', fontsize=14)

    print(f"Completed plotting {winter_year}")
    return fig


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
        for lang in ['en','ja']:  # Can be expanded to ['en', 'ja'] as needed
            # Create the full snow depth figure
            fig = plot_winter_snow_depth(winter_year, data, lang)
            
            # Create output filename for full viz
            output_file = f"{output_dir}/{lang}/full/{winter_year}.png"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Save with high DPI and tight layout
            fig.savefig(output_file, 
                dpi=100,
                bbox_inches=None,  # Do NOT use 'tight' - it can change dimensions
                pad_inches=0)
            
            # Clear the figure to free memory
            plt.close(fig)
            
            print(f"Generated full viz: {output_file}")
            
            # Create the seasons-only figure
            fig_seasons = plot_winter_seasons_only(winter_year, data, lang)
            
            # Create output filename for seasons-only viz
            output_file_seasons = f"{output_dir}/{lang}/seasons_only/{winter_year}.png"
            os.makedirs(os.path.dirname(output_file_seasons), exist_ok=True)
            
            # Save with high DPI and tight layout
            fig_seasons.savefig(output_file_seasons, 
                dpi=100,
                bbox_inches=None,  # Do NOT use 'tight' - it can change dimensions
                pad_inches=0)
            
            # Clear the figure to free memory
            plt.close(fig_seasons)
            
            print(f"Generated seasons-only viz: {output_file_seasons}")

# Run the batch process
if __name__ == "__main__":
    output_dir = "../outputs/figures"
    db_path = "../database/dist/sukayu_historical_obs_daily_everything.sqlite" 
    
    # Create output directories if they don't exist
    for lang in ['en','ja']:
    # for lang in ['en']:
        for viz_type in ['full', 'seasons_only']:
            os.makedirs(f"{output_dir}/{lang}/{viz_type}", exist_ok=True)
    
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

# Create videos if needed
if __name__ == "__main__":
    # Create videos for both visualization types in both languages
    for lang in ['en','ja']:
        for viz_type in ['full', 'seasons_only']:
            create_video_from_pngs(f"../outputs/figures/{lang}/{viz_type}/*.png", 
                                   f"../outputs/figures/{lang}/{viz_type}/winters.mp4")