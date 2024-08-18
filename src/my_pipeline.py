import pandas as pd
# import duckdb
import requests
import os
from pipeline_lib import step, DAG, sqlite
import requests
import time
from datetime import datetime
from io import StringIO


# Define cron schedule (e.g., run every hour)
CRON_HOURLY = "0 * * * *"  # This will run the pipeline at the start of every hour


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

@step(cron_schedule=CRON_HOURLY) #  This could potentially be "dag.schedule(...) instead of annotation param"
def fetch_data(source="yahoo"):
    """Fetch data from an API or CSV file."""
    print("Fetching data...")
    data = fetch_data_yahoo()
    print(str(data))

    return data

@step()
def transform_data(data): # **kwargs):
    print(f'transform_data input: {str(data)}')
    """Transform data using DuckDB."""
    print('transforming...' + str(data)) # TODO fix passing output to next func input

    df = pd.read_csv(StringIO(data))

    
    # Add a column for 2-day moving average of the 'Close' prices
    df['Close 2 Day MA'] = df['Close'].rolling(window=2).mean()
    
    return df


def sql_schema(cursor):

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

    columns = ", ".join([f"{col} {typ}" for col, typ in default_schema.items()])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS candles ({columns});"
    cursor.execute(create_table_sql)

def sql_insert(cursor):
        # Insert dummy data
    dummy_data = ('2024-08-12', 165.994995, 166.699997, 163.550003, 163.949997, 163.949997, 12435000)
    cursor.execute("INSERT INTO candles (Date, Open, High, Low, Close, Adj_Close, Volume) VALUES (?, ?, ?, ?, ?, ?, ?)", dummy_data)

def sql_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM candles")
    row_count = cursor.fetchone()[0]
    return row_count

@sqlite('data.db')
@step()
def save_data(data, **kwargs): # TODO: Handle parameter from previous func AND a @sqlite conn
    print('save')

    conn = kwargs['conn']
    cursor = conn.cursor()
    sql_schema(cursor)
    sql_insert(cursor)
    count = sql_count(cursor)
    conn.commit()

    return count

# Create a DAG
dag = DAG()

# Chain the steps with `<<`
dag << fetch_data << transform_data << save_data