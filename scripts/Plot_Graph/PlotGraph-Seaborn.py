import os
import sys
import json
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import subprocess

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
    jst
)
from i10n import TRANSLATIONS # Import the language settings/strings
# Constants
RANGE_START = 100
RANGE_END = 500
RANGE_STEP = 100
CANVAS_WIDTH = 1525  # 305 days * 5px per day
CANVAS_HEIGHT = 800  # 500px for snow + 300px for margins/labels

# Language selection function
def set_language(lang='en'):
    if lang not in TRANSLATIONS:
        raise ValueError(f"Language '{lang}' not supported. Available languages: {', '.join(TRANSLATIONS.keys())}")
    return TRANSLATIONS[lang]

def load_winter_data(json_file, winter_year):
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    winter_data = data[winter_year]
    depths = winter_data['snowdepths']
    
    milestones = {}
    for depth in range(RANGE_START, RANGE_END, RANGE_STEP):
        first_date = depths['first'][str(depth)]
        last_date = depths['last'][str(depth)]
        milestones[f'{depth}cm'] = [(first_date, last_date)] 
    
    return {
        'depths': depths,
        'milestones': milestones,
        'seasons': winter_data['scandi_season_starts']['avg_based'],
        'snowfalls': winter_data['snowfalls'],
        'max_depth': depths['max']
    }

def next_season_date(seasons, current_season):
    """Helper function to get the next season's start date"""
    season_order = ['aut', 'win', 'spr', 'sum']
    current_idx = season_order.index(current_season)
    next_idx = (current_idx + 1) % len(season_order)
    next_season = season_order[next_idx]
    
    if seasons[next_season]:
        return datetime.strptime(seasons[next_season], '%Y-%m-%d')
    return None

def plot_winter_snow_depth(winter_year, data, lang='en'):
    # Get translations for selected language
    t = set_language(lang)
    metadata = t['metadata']
    season_names = t['seasons']
    date_formats = t['date_formats']
    snowfall_markers = t['markers']
    
    # Set font based on language
    if lang == 'ja':
        plt.rcParams['font.family'] = 'SF Mono Square'
        plt.rcParams['font.size'] = 14
    else:
        plt.rcParams['font.family'] = 'SF Mono Square'
        plt.rcParams['font.size'] = 12
    
    # Set the style
    sns.set_style("whitegrid", {'grid.color': '.9'})
    
    # Create figure with pixel dimensions
    fig = plt.figure(figsize=(CANVAS_WIDTH/100, CANVAS_HEIGHT/100))
    fig.set_dpi(100)  # This makes figsize work in pixels
    
    # Create the main axis
    ax = fig.add_subplot(111)
    
    # Calculate date range
    year = int(winter_year.split('-')[0])
    start_date = datetime(year, 9, 1)
    end_date = datetime(year + 1, 7, 1)
    
    # Set axis limits
    ax.set_xlim(start_date, end_date)
    ax.set_ylim(-200, RANGE_END)
    
    # Set up grid
    ax.grid(True, linestyle=':', alpha=0.2)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_minor_locator(mdates.DayLocator())
    
    # Draw snow depth blocks
    depths = list(range(RANGE_END - RANGE_STEP, RANGE_START - RANGE_STEP, -RANGE_STEP))
    label_x = None
    
    # Find reference point for label alignment
    for depth in depths:
        if data['milestones'][f'{depth}cm'][0][0] is not None:
            start_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][0], '%Y-%m-%d')
            end_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][1], '%Y-%m-%d')
            label_x = start_date + (end_date - start_date)/2
            break
    
    if label_x is None:
        label_x = datetime(year + 1, 2, 15)
    
    # Draw blocks
    block_count = 0
    for depth in depths:
        if data['milestones'][f'{depth}cm'][0][0] is not None:
            start_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][0], '%Y-%m-%d')
            end_date = datetime.strptime(data['milestones'][f'{depth}cm'][0][1], '%Y-%m-%d')
            
            # Draw block
            ax.fill_between([start_date, end_date], [depth, depth], [0, 0],
                          color='#e6e6e6', alpha=1)
            
            # Add borders
            ax.plot([start_date, end_date], [depth, depth], color='#999999', linewidth=1)
            ax.plot([start_date, start_date], [0, depth], color='#999999', linewidth=1)
            ax.plot([end_date, end_date], [0, depth], color='#999999', linewidth=1)
            
            # Add date labels
            ax.text(start_date - timedelta(days=2), depth + 5,
                   start_date.strftime(date_formats['m_d']),
                   ha='right', va='bottom', color='#999999', size='small')
            ax.text(end_date + timedelta(days=2), depth + 5,
                   end_date.strftime(date_formats['m_d']),
                   ha='left', va='bottom', color='#999999', size='small')
            
            # Add depth labels
            if block_count > 0:
                line_start = label_x - timedelta(days=7)
                line_end = label_x + timedelta(days=7)
                ax.plot([line_start, line_end], [depth + 1, depth + 1],
                       color='#999999', linewidth=1)
                
                ax.text(label_x, depth - 10, f'{depth}cm',
                       ha='center', va='top', color='#999999',
                       bbox=dict(facecolor='#e6e6e6', edgecolor='none', pad=6))
            else:
                ax.text(label_x, depth - 10, f'{depth}cm',
                       ha='center', va='top', color='#999999')
            
            block_count += 1
    
    # Draw season blocks
    season_colors = {
        'aut': '#D9A18C',
        'win': '#94BDD1',
        'spr': '#AEC99C',
        'sum': '#F5DFA3'
    }
    
    # Draw full summer bar
    full_start = datetime(year, 9, 1)
    full_end = datetime(year + 1, 7, 1)
    ax.fill_between([full_start, full_end], [0, 0], [-50, -50],
                   color=season_colors['sum'], alpha=1.0)
    
    # Draw other seasons
    season_dates = []
    for season in ['aut', 'win', 'spr', 'sum']:
        if data['seasons'][season]:
            season_dates.append((season, datetime.strptime(data['seasons'][season], '%Y-%m-%d')))
    
    season_dates.sort(key=lambda x: x[1])
    
    if len(season_dates) >= 2:
        for i in range(len(season_dates)-1):
            season, start = season_dates[i]
            _, end = season_dates[i+1]
            
            if season != 'sum':
                ax.fill_between([start, end], [0, 0], [-50, -50],
                              color=season_colors[season], alpha=1.0)
                
                # Add labels
                label_x = start + timedelta(days=2)
                ax.text(label_x, -17, f"{season_names[season]}",
                       ha='left', va='center', color='#333333', size='small')
                ax.text(label_x, -36, start.strftime(date_formats['m_d']),
                       ha='left', va='center', color='#333333', weight='bold')
    
    # Draw snowfall markers
    markers_y_top = -52
    markers_y_bot = -110
    markers_subs_y_bot = -160
    label_offset = -10
    label_exp_offset = -20
    
    if data['snowfalls']['fst'] is not None:
        first_snow = datetime.strptime(data['snowfalls']['fst'], '%Y-%m-%d')
        ax.plot([first_snow, first_snow], [markers_y_top, markers_y_bot],
                color='#2778BE', linewidth=2, solid_capstyle='butt')
        ax.text(first_snow, markers_y_bot + label_offset,
                first_snow.strftime(date_formats['m_d']),
                ha='right', va='top', color='#2778BE', weight='bold')
        ax.text(first_snow, markers_y_bot + label_offset + label_exp_offset,
                snowfall_markers['fst'],
                ha='right', va='top', color='#2778BE', size='small')
    
    # Add other snowfall markers similarly...
    
    # Configure axes
    sns.despine(left=True)
    ax.xaxis.set_major_formatter(mdates.DateFormatter(date_formats['m']))
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=15))
    
    # Style tick labels
    ax.tick_params(axis='x', which='major', length=0, pad=10,
                  labelcolor='#999999', labelsize='small')
    ax.tick_params(axis='x', which='minor', length=10, color='#999999')
    
    for label in ax.get_xticklabels():
        label.set_bbox(dict(facecolor='white', edgecolor='none', pad=4))
    
    # Add title and metadata
    title = f"{season_names['win']} {winter_year}"
    ax.set_title(title, fontsize=16, fontweight='bold', loc='left', pad=20)
    
    total_snow = int(data["snowfalls"]["total"]) if data["snowfalls"]["total"] is not None else 0
    max_depth = int(data["max_depth"]) if data["max_depth"] is not None else 0
    
    ax.text(0, 1,
            f'{metadata["total_snowfall"]}: {total_snow:4d} cm\n{metadata["max_snow_depth"]}: {max_depth:4d} cm',
            transform=ax.transAxes, va='top', fontsize=14)
    
    return fig

def process_all_winters(json_file, output_dir):
    with open(json_file, 'r') as f:
        all_data = json.load(f)
    
    winter_years = sorted(all_data.keys())
    
    for winter_year in winter_years:
        data = load_winter_data(json_file, winter_year)
        
        for lang in ['en', 'ja']:
            fig = plot_winter_snow_depth(winter_year, data, lang)
            output_file = f"{output_dir}/{lang}/{winter_year}.png"
            
            fig.savefig(output_file,
                       dpi=100,
                       bbox_inches='tight',
                       pad_inches=0.2)
            
            plt.close(fig)
            print(f"Generated: {output_file}")

def create_video_from_pngs(input_pattern, output_file, frame_duration=1):
    ffmpeg_cmd = [
        'ffmpeg',
        '-y',
        '-framerate', f'1/{frame_duration}',
        '-pattern_type', 'glob',
        '-i', input_pattern,
        '-c:v', 'libx264',
        '-pix_fmt', 'yuv420p',
        '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2,fps=30',
        output_file
    ]
    
    try:
        subprocess.run(ffmpeg_cmd, check=True)
        print(f"Video created successfully: {output_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error creating video: {e}")

if __name__ == "__main__":
    # Set up directories
    output_dir = "../outputs/figures"
    output_dir_en = "../outputs/figures/en"
    output_dir_ja = "../outputs/figures/ja"
    json_file = "../outputs/derived/Sukayu-Winters-Data.json"
    
    # Create output directories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_en, exist_ok=True)
    os.makedirs(output_dir_ja, exist_ok=True)
    
    # Generate plots
    process_all_winters(json_file, output_dir)
    
    # Create videos
    create_video_from_pngs("../outputs/figures/en/*.png", "../outputs/figures/en/winters.mp4")
    create_video_from_pngs("../outputs/figures/ja/*.png", "../outputs/figures/ja/winters.mp4")