# Debuggable Data Pipelines

Tiny data pipelines optimized for debugability. Lightweight [Airflow](https://airflow.apache.org/) or [Prefect](https://www.prefect.io/) alternative. 

```
@step() annotation: catch exceptions and save a snapshot of the input
pipeline_lib.py: CLI for running, debugging, or scheduling pipelines
```

## Example

```
from pipeline_lib import step, Pipeline, PipelineConfig, StepConfig, sqlite

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


SCHEMA_CANDLES = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "Close": "REAL",
    "Signal_MA": "REAL",
}


@step()
@sqlite(db_name="data.db", table_schema=SCHEMA_CANDLES)
def save_data(cursor, data, **kwargs):
    sql_insert(cursor, data)  # Steps must be idempotent
    count = sql_count(cursor)
    print(f'Rows in data.db: {count}')
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
# python3 src/pipeline_lib.py --file examples/my_pipeline.py --run
```

## CLI
```
python3 pipeline_lib.py --file path/to/pipeline.py [options]

--run: Run pipeline
--debug [snapshot.pkl]: Rerun a failed step snapshot

--enable: WIP add to system cron
--disable: WIP remove from system cron
```
## Run tests
```
pytest src/test_pipeline.py  
```

## Follow ups
- How to make snapshotting work with unpickleable fields
- Implement --enable and --disable CLI flags for managing system cron
- Store pipeline history in a sqlite db in user home dir
- Terminal UI for showing pipeline history
