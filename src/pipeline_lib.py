import logging
import argparse
import os
import importlib.util
import sys
from functools import wraps
from crontab import CronTab
import pickle
import sqlite3
import datetime

# TODO:
# passing return value to next step parameter needs to work
# make the adding to cron and removing and pritnging existing crons work
# the cron should prboably go into a Pipeline member instead of task

class DefaultArgs:
    def __init__(self):
        self.snapshot = True
        self.run = True
        self.check_errors = False
        self.setup_cron = False
        self.list_cron = False
## args =  DefaultArgs()
save_snapshots = False

# TODO: So we should be saving snapshots for each run but only write them if there is an exception at any point
# The exception name should be in the filename, and if necessary a counter appended for multiple eceptions
def snapshot_state(filename, **kwargs) -> None:
    # TODO: Snapshot a substring or random sample instead of entire dataframe
    timestamp = datetime.datetime.now().strftime("%y%m%d%H%M%S")
    with open(f"snapshots/{filename}_{timestamp}.pkl", "wb") as f:

        if 'conn' in kwargs: del kwargs['conn']

        pickle.dump(obj=kwargs, file=f)

def step():
    """Decorator to log errors, handle exceptions, and optionally set a cron schedule."""
    def decorator(func):
        print(f"Load {func.__name__}")
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                print(f"{func.__name__} input: {args} {kwargs}")
                global save_snapshots
                if save_snapshots:
                    snapshot_state(func.__name__ + "_input", kwargs=kwargs)
                result = func(*args, **kwargs)
                print(f"{func.__name__} output: {result}")
                if save_snapshots:
                    snapshot_state(func.__name__ + "_output", **kwargs)
                return result
            except Exception as e:
                logging.error(f"Error in step '{func.__name__}': {e}")
                raise
        return wrapper
    return decorator



class Pipeline:
    """Class to represent a Directed Acyclic Graph of steps."""
    def __init__(self, schedule: str = None):
        self.schedule = schedule
        self.steps = []

    def __lshift__(self, func):
        """Chain functions with << operator."""
        self.steps.append(func)
        return self

    def run(self):
        """Run all steps in the Pipeline sequentially."""
        if not self.steps:
            raise ValueError("No steps to run in the Pipeline.")
        
        result = None
        for step_func in self.steps:
            print(f'Pipeline::run result {result}')
            if result is None:
                result = step_func()
            else:
                result = step_func(result)
        return result


def sql_schema(cursor, schema):
    columns = ", ".join([f"{col} {typ}" for col, typ in schema.items()])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS candles ({columns});"
    cursor.execute(create_table_sql)


def sqlite(file_path, table_schema = None):
    """Decorator to inject a SQLite connection."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            conn = sqlite3.connect(file_path)
            try:
                if table_schema: sql_schema(conn.cursor(), table_schema)

                # Inject the connection into the kwargs
                kwargs['conn'] = conn
                result = func(*args, **kwargs)
            finally:
                conn.close()
            return result
        return wrapper
    return decorator


def setup_cronjob(cron_schedule, pipeline_file) -> None:
    """Set up the cron job for the pipeline."""
    cron = CronTab(user=True)
    command = f"python {pipeline_file} --run" # TODO fix
    job = cron.new(command=command)
    job.setall(cron_schedule)
    cron.write()
    print(f"Cron job set up: {command} at schedule '{cron_schedule}'")


def load_pipeline(pipeline_file):
    # Dynamically import the domain-specific pipeline file
    spec = importlib.util.spec_from_file_location("pipeline_module", pipeline_file)
    pipeline_module = importlib.util.module_from_spec(spec)
    sys.modules["pipeline_module"] = pipeline_module
    spec.loader.exec_module(pipeline_module)
    return pipeline_module


def main(args):
    global save_snapshots
    save_snapshots = args.snapshot

    pipeline_module =   load_pipeline(args.file)

    # pipeline_module = load_pipeline("./src/my_pipeline.py")
   

    if args.run:
        pipeline_module.Pipeline.run()
    if args.debug:
        # TODO
        pass
    if args.enable:
        # If a cron_schedule is provided in the pipeline file, set up the cron job
        # TODO: cron_schedule should come as a member of Pipeline
        #cron_schedule = getattr(pipeline_module, 'cron_schedule', None)
        cron_schedule = pipeline_module.Pipeline.schedule
        if cron_schedule:
            setup_cronjob(cron_schedule, args.file)
        else:
            print("No cron schedule found in the pipeline file.")
    if args.disable:
        # TODO
        pass


# TODO: TUI to render pipeline steps (name, inputs, and outputs)
# and to render scheduled or currently running cron jobs
def run_cli():
    """Command-line interface for running the pipeline."""
    parser = argparse.ArgumentParser(description="Pipeline CLI")
    parser.add_argument('--file', required=True, help="Path to the pipeline file")

    # Execution
    parser.add_argument('--run', action='store_true', help="Run the pipeline")
    parser.add_argument('--debug', action='store_true', help="Rerun last failing snapshot")

    # Scheduling
    parser.add_argument('--enable', action='store_true', help="Set up the cron job for this pipeline")
    parser.add_argument('--disable', action='store_true', help="Remove cron job for this pipeline")

    # Debugging
    parser.add_argument('--snapshot', action='store_true', help="Run pipeline and save inputs and outputs")


    # Override default args
    args = parser.parse_args()
    main(args)
 


if __name__ == "__main__":
    run_cli()