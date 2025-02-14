import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json

# Add the parent directory of utils.py to the system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Add the utilities directory to the system path
script_dir = os.path.dirname(os.path.abspath(__file__))
utilities_dir = os.path.join(script_dir, '../utilities')
sys.path.append(utilities_dir)

from utils import (db_connect, db_query_data)


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))


db_path = os.path.join(script_dir, '../../database/dist/sukayu_historical_obs_daily_everything.sqlite')
output_path = os.path.join(script_dir, '../../outputs/heatmaps')


def generate_heatmaps():
    # Connect to the database
    conn = db_connect(db_path)
    if conn is None:
        return

    # Query the data
    query = """
    SELECT 
        obs_date, 
        temp_avg 
    FROM 
        obs_sukayu_daily
    """
    df = db_query_data(conn, query)

    # Close the database connection
    conn.close()

    # Create output directory if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Loop through the rows and generate a single-row heatmap for each one
    for index, row in df.iterrows():
        obs_date = row['obs_date']
        temp_avg = row['temp_avg']

        print (obs_date, temp_avg)


def generate_html():
    # Create output directory if it doesn't exist
    output_dir = output_path
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get the list of heatmap PNG files
    heatmap_files = sorted([f for f in os.listdir(output_dir) if f.endswith('.png')], reverse=True)

    # Generate the HTML content
    html_content = '<html><head><title>Winter Season Heatmaps</title></head><body>'
    html_content += '<h1>Winter Season Heatmaps</h1>'
    for file in heatmap_files:
        html_content += f'<h2>{file[:-4]}</h2>'
        html_content += f'<img src="{file}" alt="{file[:-4]}">'
    html_content += '</body></html>'

    # Save the HTML file
    with open(os.path.join(output_dir, 'index.html'), 'w') as file:
        file.write(html_content)

if __name__ == "__main__":
    generate_heatmaps()
    generate_html()