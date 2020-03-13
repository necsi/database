#!/usr/bin/python
#
# Author: Michael Buchel
# Company: MIM Technology Group Inc.
# Reason: Pulls data from john hopkins time series data
#
import csv
import codecs
import numpy as np
import pandas as pd
from datetime import datetime
from utils import download_csv_to_dataframe

# Constants for the code, should not need to change
TIME_SERIES_CONFIRMED_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv"
TIME_SERIES_RECOVERED_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"
TIME_SERIES_DEAD_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv"

if __name__ == "__main__":
    confirmed = download_csv_to_dataframe(TIME_SERIES_CONFIRMED_URL)
    recovered = download_csv_to_dataframe(TIME_SERIES_RECOVERED_URL)
    dead = download_csv_to_dataframe(TIME_SERIES_DEAD_URL)
    new_df = pd.DataFrame(columns = ['date', 'city', 'province', 'country', 'lat/long', 'confirmed', 'recovered', 'dead', 'daily_diff_confirm', 'daily_diff_recover', 'daily_diff_dead'])
    pos = 0
    dates = confirmed.columns
    confirm_rows = confirmed.values
    recover_rows = recovered.values
    dead_rows = dead.values

    # Iterates through values
    for ind, j in enumerate(confirm_rows):
        loc_city_province = j[0].split(", ")
        city = loc_city_province[0]
        province = ""
        if len(loc_city_province) == 2:
            province = loc_city_province[1]
        country = j[1]
        latlong = j[2] + "," + j[3]
        # Iterates through dates
        for i in range(4, len(dates)):
            daily_diff_confirm = -1
            daily_diff_recover = -1
            daily_diff_dead = -1
            if i > 4:
                if confirm_rows[ind][i] != "" and confirm_rows[ind][i - 1] != "":
                    daily_diff_confirm = int(confirm_rows[ind][i]) - int(confirm_rows[ind][i - 1])
                if recover_rows[ind][i] != "" and recover_rows[ind][i - 1] != "":
                    daily_diff_recover = int(recover_rows[ind][i]) - int(recover_rows[ind][i - 1])
                if dead_rows[ind][i] != "" and dead_rows[ind][i - 1] != "":
                    daily_diff_dead = int(dead_rows[ind][i]) - int(dead_rows[ind][i - 1])
            new_df.loc[pos] = [dates[i], city, province, country, latlong, confirm_rows[ind][i], recover_rows[ind][i], dead_rows[ind][i], daily_diff_confirm, daily_diff_recover, daily_diff_dead]
            pos = pos + 1
    print(new_df)
    new_df.to_csv("time_series.csv", index = False)
