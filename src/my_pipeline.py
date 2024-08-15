import pandas as pd
# import duckdb
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
    print("Fetching data...")
    return 1

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