# Evolving Data Pipelines (Overlord)

Human-first task execution, scheduling, and monitoring.

# Example

 # Run the pipeline at the start of every hour
@step(cron_schedule= "0 * * * *")
def fetch_data(source="yahoo"):
    data = requests.get("...")
    return data

@step()
def transform_data(**kwargs):
    data.dropna()
    return data

@step()
def save_data(**kwargs):
    duckdb.save()


dag = DAG()
dag << fetch_data << transform_data << save_data

# CLI

--file my_pipeline.py
--run
--debug: Rerun the last failing snapshot
--setup-cron: Set up a cron job for this pipeline
--list-cron: List all cron jobs for this pipeline
--check-errors: Check for errors in the log
