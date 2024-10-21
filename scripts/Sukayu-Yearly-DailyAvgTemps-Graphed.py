import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the SQLite database
conn = sqlite3.connect('database/sukayu_historical_obs_daily.sqlite')

# Query the data
query = "SELECT obs_date, temp_avg FROM obs_sukayu_daily"
df = pd.read_sql_query(query, conn)

# Convert the date column to datetime
df['date'] = pd.to_datetime(df['obs_date'])

# Extract the day of the year
df['day_of_year'] = df['date'].dt.dayofyear

# Extract the year
df['year'] = df['date'].dt.year

# Ensure temp_avg is numeric and drop rows with NaN values
df['temp_avg'] = pd.to_numeric(df['temp_avg'], errors='coerce')
df.dropna(subset=['temp_avg'], inplace=True)

# Plot the daily average temperature for each year
plt.figure(figsize=(15, 8))
for year in df['year'].unique():
    yearly_data = df[df['year'] == year]
    plt.plot(yearly_data['day_of_year'], yearly_data['temp_avg'], label=str(year))

plt.xlabel('Day of the Year')
plt.ylabel('Average Temperature')
plt.title('Daily Average Temperature Year by Year')
plt.legend(title='Year')
plt.show()

# Close the connection
conn.close()