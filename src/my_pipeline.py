import pandas as pd
# import duckdb
import requests
import os
from pipeline_lib import step, DAG
import requests
import time
from datetime import datetime


# Define cron schedule (e.g., run every hour)
cron_schedule = "0 * * * *"  # This will run the pipeline at the start of every hour

# Paths
csv_input = 'input.csv'
csv_output = 'output.csv'
tmp_output = 'tmp_output.csv'

def fetch_data_yahoo():
    # Define the stock ticker and date range
    ticker = "GOOG"
    start_date = "2024-07-14"  # One year ago from today
    end_date = "2024-08-14"    # Today
    # Convert dates to UNIX timestamps
    start_timestamp = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    end_timestamp = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple()))

    # Yahoo Finance URL for historical data
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_timestamp}&period2={end_timestamp}&interval=1d&events=history&includeAdjustedClose=true"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    # Send GET request to Yahoo Finance with the headers
    response = requests.get(url, headers=headers)
    return response.text

@step(cron_schedule=cron_schedule)
def fetch_data(source="yahoo"):
    """Fetch data from an API or CSV file."""
    print("Fetching data...")

    data = fetch_data_yahoo()
    print(str(data))

    return data

@step()
def transform_data(df_input):
    """Transform data using DuckDB."""
    print('transforming...')
    return 2

@step()
def save_data(df_merged):
    print('save')
    return 3

# Create a DAG
dag = DAG()

# Chain the steps with `<<`
dag << fetch_data << transform_data << save_data