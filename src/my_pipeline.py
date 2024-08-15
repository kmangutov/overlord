import pandas as pd
import duckdb
import requests
import os
from pipeline_lib import step, DAG

# Define cron schedule (e.g., run every hour)
cron_schedule = "0 * * * *"  # This will run the pipeline at the start of every hour

# Paths
csv_input = 'input.csv'
csv_output = 'output.csv'
tmp_output = 'tmp_output.csv'

@step(cron_schedule=cron_schedule)
def fetch_data(source=None):
    """Fetch data from an API or CSV file."""
    if source == 'api':
        response = requests.get('https://api.example.com/data')
        response.raise_for_status()
        df = pd.DataFrame(response.json())
    else:
        df = pd.read_csv(source or csv_input)
    return df

@step()
def transform_data(df_input):
    """Transform data using DuckDB."""
    conn = duckdb.connect(database=':memory:')
    conn.execute(f"""
        CREATE TABLE new_data AS SELECT * FROM df_input;
        CREATE TABLE old_data AS SELECT * FROM read_csv_auto('{csv_output}');
        CREATE TABLE merged AS 
        SELECT * FROM new_data
        WHERE timestamp NOT IN (SELECT timestamp FROM old_data);
    """)
    df_merged = conn.execute("SELECT * FROM merged").fetchdf()
    conn.close()
    return df_merged

@step()
def save_data(df_merged):
    """Save or append transformed data to the output CSV."""
    if os.path.exists(csv_output):
        df_merged.to_csv(tmp_output, mode='a', header=False, index=False)
        os.system(f"cat {tmp_output} >> {csv_output} && rm {tmp_output}")
    else:
        df_merged.to_csv(csv_output, index=False)

# Create a DAG
dag = DAG()

# Chain the steps with `<<`
dag << fetch_data << transform_data << save_data