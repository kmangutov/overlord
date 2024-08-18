import pandas as pd
import requests
import time
from datetime import datetime
from io import StringIO
from pipeline_lib import step, Pipeline, PipelineConfig, StepConfig, sqlite_connection

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
    return data

@step()
def transform_data(data):
    print(f'transform_data input: {str(data)}')

    df = pd.read_csv(StringIO(data))
    
    # Add a column for 2-day moving average of the 'Close' prices
    df['Signal_MA'] = df['Close'].rolling(window=2).mean()
    df = df.dropna()
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

@step()
def save_data(data):
    with sqlite_connection('data.db', table_schema=SCHEMA_CANDLES) as conn:
        cursor = conn.cursor()
        sql_insert(cursor, data)
        count = sql_count(cursor)
        print(f'Rows in data.db: {count}')
        conn.commit()
    return count

# Create a PipelineConfig
pipeline_config = PipelineConfig(
    steps=[
        StepConfig(func=fetch_data, name="Fetch Data"),  # TODO: Can't we infer name from the function name? ast should be able to parse the function docstring etc
        StepConfig(func=transform_data, name="Transform Data"),
        StepConfig(func=save_data, name="Save Data"),
    ],
    schedule=CRON_HOURLY
)

# Create a Pipeline
pipeline = Pipeline(pipeline_config)

if __name__ == "__main__":
    result = pipeline.run()
    print(f"Pipeline completed. Final result: {result}")