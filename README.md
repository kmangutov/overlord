# Evolving Data Pipelines

Tiny data pipelines optimized for debugability

## Example

```
SCHEMA_CANDLES = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "Close": "REAL",
    "Signal_MA": "REAL",
}


@step()
def fetch_data():
    """Fetch data from an API or CSV file."""
    print("Fetching data...")
    data = requests.get(url)
    return data

@step()
def transform_data(data, **kwargs):
    df = pd.read_csv(StringIO(data))
    df = df.dropna()
    return df

@step()
def save_data(data, **kwargs):
    with sqlite_connection('data.db', table_schema=SCHEMA_CANDLES) as conn:
        cursor = conn.cursor()
        sql_insert(cursor, data)
        count = sql_count(cursor)
        print(f'Rows in data.db: {count}')
        conn.commit()

    return count


pipeline_config = PipelineConfig(
    steps=[
        StepConfig(func=fetch_data),
        StepConfig(func=transform_data),
        StepConfig(func=save_data),
    ],
    schedule="0 * * * *"  # This will run the pipeline at the start of every hour 
)


pipeline = Pipeline(pipeline_config)

```

## CLI
```
python3 pipeline_lib.py --file path/to/pipeline.py [options]

--run: Run pipeline
--debug [snapshot.pkl]: Rerun a failed step snapshot

--enable: WIP add to system cron
--disable: WIP remove from system cron
```
