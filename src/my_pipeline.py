import pandas as pd
# import duckdb
import requests
import os
from pipeline_lib import step, Pipeline, sqlite
import requests
import time
from datetime import datetime
from io import StringIO


SCHEMA_CANDLES = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "Date": "TEXT",  # or "DATE"
    "Open": "REAL",
    "High": "REAL",
    "Low": "REAL",
    "Close": "REAL",
    "Adj_Close": "REAL",
    "Volume": "INTEGER",
}

# Define cron schedule (e.g., run every hour)
CRON_HOURLY = "0 * * * *"  # This will run the pipeline at the start of every hour


def fetch_data_yahoo():
    # Define the stock ticker and date range
    ticker = "GOOG"
    start_date = "2024-08-7"  # One year ago from today
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

@step()
def fetch_data():
    """Fetch data from an API or CSV file."""
    print("Fetching data...")
    data = fetch_data_yahoo()
    print(str(data))

    return data

@step()
def transform_data(data):
    print(f'transform_data input: {str(data)}')

    df = pd.read_csv(StringIO(data))
    
    # Add a column for 2-day moving average of the 'Close' prices
    df['Signal_MA'] = df['Close'].rolling(window=2).mean()
    df.dropna()
    return df


def sql_insert(cursor):
    dummy_data = ('2024-08-12', 165.994995, 166.699997, 163.550003, 163.949997, 163.949997, 12435000)

    columns = ", ".join([col for col in SCHEMA_CANDLES.keys() if col != "id"])
    placeholders = ", ".join(["?"] * len(dummy_data))

    cursor.execute(f"INSERT INTO candles ({columns}) VALUES ({placeholders})", dummy_data)


def sql_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM candles")
    row_count = cursor.fetchone()[0]
    return row_count


@sqlite('data.db', table_schema=SCHEMA_CANDLES)
@step()
def save_data(data, **kwargs):
    print('save')

    conn = kwargs['conn']
    cursor = conn.cursor()
    sql_insert(cursor)  # TODO: Insert parameter instead of a placeholder
    count = sql_count(cursor)
    conn.commit()

    return count

# Create a Pipeline
Pipeline = Pipeline(schedule = CRON_HOURLY)

# Chain the steps with `<<`
Pipeline << fetch_data << transform_data << save_data