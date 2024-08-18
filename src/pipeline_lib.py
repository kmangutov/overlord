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
from dataclasses import dataclass
from typing import Callable, List, Optional, Dict, Any
import contextlib


@dataclass
class StepConfig:
    """Configuration for a pipeline step."""
    func: Callable

@dataclass
class PipelineConfig:
    """Configuration for a pipeline."""
    steps: List[StepConfig]
    schedule: Optional[str] = None

class Pipeline:
    """Class to represent a Directed Acyclic Graph of steps."""

    def __init__(self, config: PipelineConfig):
        """
        Initialize the Pipeline.

        Args:
            config (PipelineConfig): The configuration for the pipeline.
        """
        self.config = config

    def run(self) -> Any:
        """
        Run all steps in the Pipeline sequentially.

        Returns:
            Any: The result of the last step in the pipeline.

        Raises:
            ValueError: If there are no steps to run in the Pipeline.
        """
        if not self.config.steps:
            raise ValueError("No steps to run in the Pipeline.")
        
        result = None
        for step in self.config.steps:
            print(f'Pipeline::run step: {step.func.__name__}')
            result = step.func(result) if result is not None else step.func()
        return result

@contextlib.contextmanager
def sqlite_connection(file_path: str, table_schema: Optional[Dict[str, str]] = None):
    """
    Context manager for SQLite connections.

    Args:
        file_path (str): Path to the SQLite database file.
        table_schema (Optional[Dict[str, str]]): Schema for table creation.

    Yields:
        sqlite3.Connection: The SQLite connection.
    """
    conn = sqlite3.connect(file_path)
    try:
        if table_schema:
            columns = ", ".join([f"{col} {typ}" for col, typ in table_schema.items()])
            create_table_sql = f"CREATE TABLE IF NOT EXISTS candles ({columns});"
            conn.execute(create_table_sql)
        yield conn
    finally:
        conn.close()

def step(debug=False):
    """
    Decorator to log errors, handle exceptions, and optionally set a cron schedule.

    Args:
        name (Optional[str]): Name of the step. If None, the function name will be used.

    Returns:
        Callable: The decorated function.
    """
    # TODO: optional timeout parameter
    def decorator(func: Callable) -> Callable:
        step_name = func.__name__
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                logging.info(f"{step_name} input: {args} {kwargs}")
                result = func(*args, **kwargs)
                logging.info(f"{step_name} output: {result}")
                return result
            except Exception as e:
                logging.error(f"Error in step '{step_name}': {e}")
                if not debug:
                    # TODO: The pipeline name should also be in the file...
                    snapshot_state(f"{step_name}_{type(e).__name__}", state={'step_name': step_name,'input': (args, kwargs), 'exception': e})
                raise
        return wrapper
    return decorator


# TODO: The file name should be the name of the step and any exceptions or stacktraces
def snapshot_state(filename: str, state: dict) -> None:
    """
    Save a snapshot of the current state.

    Args:
        filename (str): Name of the snapshot file.
        *args: Positional arguments to save.
        **kwargs: Keyword arguments to save.
    """
    with open(f"snapshots/{filename}.pkl", "wb") as f:
        pickle.dump(state, f)

def setup_cronjob(cron_schedule: str, pipeline_file: str) -> None:
    """
    Set up the cron job for the pipeline.

    Args:
        cron_schedule (str): The cron schedule string.
        pipeline_file (str): Path to the pipeline file.
    """
    cron = CronTab(user=True)
    command = f"python {pipeline_file} --run"
    job = cron.new(command=command)
    job.setall(cron_schedule)
    cron.write()
    logging.info(f"Cron job set up: {command} at schedule '{cron_schedule}'")



def load_pipeline(pipeline_file: str) -> Any:
    """
    Dynamically import the domain-specific pipeline file.

    Args:
        pipeline_file (str): Path to the pipeline file.

    Returns:
        Any: The imported pipeline module.
    """
    spec = importlib.util.spec_from_file_location("pipeline_module", pipeline_file)
    pipeline_module = importlib.util.module_from_spec(spec)
    sys.modules["pipeline_module"] = pipeline_module
    spec.loader.exec_module(pipeline_module)
    return pipeline_module


def debug_step(snapshot_file: str, pipeline_module):
    """
    Load a snapshot from the specified file and re-execute the step function with the stored input parameters.

    Args:
        snapshot_file (str): Path to the snapshot file.
    """
    with open(snapshot_file, 'rb') as f:
        state = pickle.load(f)
    
    print(f'pickle.load state: {state}')

    func_name = state['step_name']
    input = state['input']
    # Dynamically retrieve the function from the loaded pipeline module
    func = getattr(pipeline_module, func_name, None)
    
    if func is None:
        raise ValueError(f"Function '{func_name}' not found in the current scope.")
    
    # Execute the function with the stored parameters
    try:
        result = func(input[0][0], debug=True) # Unholy 
        print(f"Debug result: {result}")
    except Exception as e:
        print(f"Debug failed: {e}")




def main(args: argparse.Namespace) -> None:
    """
    Main function to handle CLI arguments and run the pipeline.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.
    """
    pipeline_module = load_pipeline(args.file)


    if args.debug:
        if os.path.exists(args.debug):
            debug_step(args.debug, pipeline_module=pipeline_module)
        else:
            print(f"Snapshot file {args.debug} not found.")

    if args.run:
        pipeline_module.pipeline.run()
    if args.enable:
        if pipeline_module.pipeline.config.schedule:
            setup_cronjob(pipeline_module.pipeline.config.schedule, args.file)
        else:
            logging.warning("No cron schedule found in the pipeline file.")
    if args.disable:
        # TODO: Implement cron job removal
        pass

def run_cli() -> None:
    """Command-line interface for running the pipeline."""
    parser = argparse.ArgumentParser(description="Pipeline CLI")
    parser.add_argument('--file', required=True, help="Path to the pipeline file")
    parser.add_argument('--run', action='store_true', help="Run the pipeline")
    parser.add_argument("--debug", type=str, help="Specify the snapshot file to debug.")
   
    parser.add_argument('--enable', action='store_true', help="Set up the cron job for this pipeline")
    parser.add_argument('--disable', action='store_true', help="Remove cron job for this pipeline")

    args = parser.parse_args()
    main(args)

if __name__ == "__main__":
    run_cli()