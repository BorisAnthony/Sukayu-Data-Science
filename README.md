![Sukayu Onsen]('./assets/Sukayu-Photo.jpeg')

# Sukayu Seasonal Weather Observations Analysis

This project analyses various databases of historical weather observation data from the JMA (Japan Meteorological Association) station at Sukayu Onsen, Aomori, Japan.

As one of the oldest offical weather stations in Japan, and also at the foot of the Hakkoda Mountains, known to be one of the snowiest places in the country, we hope to glean some trends in the effects of climate change on the area's winter mountain sport seasons.


# What are the various dates in "Sukayu-Seasonal-Dates.json"

## "Scandi Seasons"
What we call "Scandi Seasons" here is inspired by a method used in Scandinavia to identify when the seasons started from a meteorological point of view (instead of astrological).

We calculate 2 versions, one based on the daily mean temperature for 7 days, so:

- The First Day of Autumn is the first day that the daily mean temperature remained below 10°C for 7 consecutive days.
- The First Day of Winter is the first day that the daily mean temperature remained below  0°C for 7 consecutive days.
- The First Day of Spring is the first day that the daily mean temperature remained above  0°C for 7 consecutive days.
- The First Day of Summer is the first day that the daily mean temperature remained above 10°C for 7 consecutive days.

… and the other based on the average daily mean temperature across 7 days, so:

- The First Day of Autumn is the first day that the average of following 7 days' daily mean temperature remained below 10°C.
- The First Day of Winter is the first day that the average of following 7 days' daily mean temperature remained below  0°C.
- The First Day of Spring is the first day that the average of following 7 days' daily mean temperature remained above  0°C.
- The First Day of Summer is the first day that the average of following 7 days' daily mean temperature remained above 10°C.

## Snowfalls

First and last snowfalls are simply the first time in the last quarter of a year, and the last day in the first quarter of the following year, when a snowfall greater than 0 was reported.

"First and Last of Consequence" is similar to the "Scandi Seasons", where we look for the first and last time 3 days in a row of snowfall were recorded that season.


## Snowdepths

Dates for each season when the snowdepth was first reported to surpass multiples of 100cm depths.
Also, date of the day after the last reported snowdepth of the season. 


---

# Some things to notice

Until 2008, it was very rare, almost unheard of, to have 7 consecutive days in summer where the daily average temperature remained above 10°C. In those cases, "Scandi Summer" never even started!



---

# How to install & run

This project currently is based in the Python programming language and requires familarity with environment setup.
I will try to lightly document easy setup instructions eventually.