import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json

def generate_heatmaps():
    # Load the JSON data
    with open('../../outputs/derived/Sukayu-Winters-Data.json', 'r') as file:
        data = json.load(file)

    # Create output directory if it doesn't exist
    output_dir = '../../outputs/heatmaps'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate heatmaps for each winter season
    for season, season_data in data.items():
        # Extract daily average temperatures
        temps = season_data['temps']['avg']['avg']

        # Create a DataFrame for the heatmap
        dates = pd.date_range(start=f'{season[:4]}-09-01', end=f'{int(season[:4])+1}-06-30')
        df = pd.DataFrame({'Date': dates, 'Temperature': temps})
        df['Month'] = df['Date'].dt.month
        df['Day'] = df['Date'].dt.day

        # Pivot the DataFrame to create a matrix for the heatmap
        heatmap_data = df.pivot('Day', 'Month', 'Temperature')

        # Create the heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(heatmap_data, cmap='coolwarm', cbar_kws={'label': 'Temperature (°C)'})
        plt.title(f'Winter Season {season} Daily Average Temperatures')
        plt.xlabel('Month')
        plt.ylabel('Day')
        plt.xticks(ticks=range(1, 11), labels=['Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'])
        plt.yticks(rotation=0)

        # Save the heatmap as a PNG file
        plt.savefig(os.path.join(output_dir, f'{season}.png'))
        plt.close()

def generate_html():
    # Create output directory if it doesn't exist
    output_dir = '../../outputs/heatmaps'
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
