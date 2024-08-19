"""
python3 src/pipeline_lib.py --file examples/my_pipeline.py --run
python3 src/pipeline_lib.py --file examples/my_pipeline.py --debug [snapshot.pkl]
"""

import pandas as pd
import requests
import time
from datetime import datetime
from io import StringIO
from pipeline_lib import step, Pipeline, PipelineConfig, StepConfig, sqlite

SCHEMA_CANDLES = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "Date": "TEXT",
    "Open": "REAL",
    "High": "REAL",
    "Low": "REAL",
    "Close": "REAL",
    "Adj_Close": "REAL",
    "Volume": "INTEGER",
    "Signal_MA": "REAL",
}

# Define cron schedule (e.g., run every hour)
CRON_HOURLY = "0 * * * *"  # This will run the pipeline at the start of every hour

def fetch_data_yahoo():
    # Define the stock ticker and date range
    ticker = "GOOG"
    start_date = "2024-08-7"
    end_date = "2024-08-14"
    start_timestamp = int(time.mktime(datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    end_timestamp = int(time.mktime(datetime.strptime(end_date, "%Y-%m-%d").timetuple()))

    url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_timestamp}&period2={end_timestamp}&interval=1d&events=history&includeAdjustedClose=true"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    return response.text

@step()
def fetch_data():
    """Fetch data from an API or CSV file."""
    print("Fetching data...")
    data = fetch_data_yahoo()
    print(str(data))

    # TODO: Snapshot saving wont work if there is no input right now

    return data

@step()
def transform_data(data, **kwargs):
    print(f'transform_data input: {str(data)}')

    df = pd.read_csv(StringIO(data))
    
    # Add a column for 2-day moving average of the 'Close' prices
    df['Signal_MA'] = df['Close'].rolling(window=2).mean()
    df = df.dropna()

    # die = 1 / 0

    return df

def sql_insert(cursor, data):
    columns = ", ".join([col for col in SCHEMA_CANDLES.keys() if col != "id"])
    placeholders = ", ".join(["?"] * (len(SCHEMA_CANDLES) - 1))

    for _, row in data.iterrows():
        cursor.execute(f"INSERT INTO candles ({columns}) VALUES ({placeholders})", tuple(row))

def sql_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM candles")
    row_count = cursor.fetchone()[0]
    return row_count

# Biggest TODO right now: But nothing stateful e.g. sql write should work in debug mode!!!
# There should be a @stateful annotation? maybe that would mock the conn? Idk
@step()
@sqlite(db_name="data.db", table_schema=SCHEMA_CANDLES)
def save_data(cursor, data, **kwargs):
    sql_insert(cursor, data)
    count = sql_count(cursor)
    print(f'Rows in data.db: {count}')
    return count

# Create a PipelineConfig
pipeline_config = PipelineConfig(
    steps=[
        StepConfig(func=fetch_data),
        StepConfig(func=transform_data),
        StepConfig(func=save_data),
    ],
    schedule=CRON_HOURLY
)

# Create a Pipeline
pipeline = Pipeline(pipeline_config)
