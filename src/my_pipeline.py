import pandas as pd
# import duckdb
import requests
import os
from pipeline_lib import step, DAG, sqlite
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
    start_date = "2024-08-10"  # One year ago from today
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
def transform_data(**kwargs):
    print(f'transform_data input: {str(kwargs)}')
    """Transform data using DuckDB."""
    print('transforming...')
    return 2


@sqlite('data.db')
@step()
def save_data(conn, **kwargs):
    print('save')

    default_schema = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "Date": "TEXT",  # or "DATE"
        "Open": "REAL",
        "High": "REAL",
        "Low": "REAL",
        "Close": "REAL",
        "Adj_Close": "REAL",
        "Volume": "INTEGER"
    }

    # Create table if it doesn't exist
    cursor = conn.cursor()
    columns = ", ".join([f"{col} {typ}" for col, typ in default_schema.items()])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS candles ({columns});"
    cursor.execute(create_table_sql)
    conn.commit()


    # Insert dummy data
    dummy_data = ('2024-08-12', 165.994995, 166.699997, 163.550003, 163.949997, 163.949997, 12435000)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO candles (Date, Open, High, Low, Close, Adj_Close, Volume) VALUES (?, ?, ?, ?, ?, ?, ?)", dummy_data)
    conn.commit()
    
    print(f"Inserted dummy data: {dummy_data}")
    return 3

# Create a DAG
dag = DAG()

# Chain the steps with `<<`
dag << fetch_data << transform_data << save_data