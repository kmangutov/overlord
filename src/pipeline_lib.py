import logging
import argparse
import os
import importlib.util
import sys
from functools import wraps
from crontab import CronTab


step_functions = []
def step(cron_schedule=None):
    """Decorator to log errors, handle exceptions, and optionally set a cron schedule."""
    def decorator(func):
        step_functions.append(func.__name__)  # Track the step function names
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logging.error(f"Error in step '{func.__name__}': {e}")
                raise
        return wrapper
    return decorator

class DAG:
    """Class to represent a Directed Acyclic Graph of steps."""
    def __init__(self):
        self.steps = []

    def __lshift__(self, func):
        """Chain functions with << operator."""
        self.steps.append(func)
        return self

    def run(self):
        """Run all steps in the DAG sequentially."""
        if not self.steps:
            raise ValueError("No steps to run in the DAG.")
        
        result = None
        for step_func in self.steps:
            if result is None:
                result = step_func()
            else:
                result = step_func(result)

def list_steps():
    """List all steps annotated with @step."""
    print("Steps in the pipeline:")
    for step_func in step_functions:
        print(f"- {step_func}")

def check_errors(logfile):
    """Check the log file for any errors."""
    if os.path.exists(logfile):
        with open(logfile, 'r') as f:
            errors = f.readlines()
        if errors:
            print("Errors found in the log:")
            for error in errors:
                print(error.strip())
        else:
            print("No errors found in the log.")
    else:
        print("Log file not found.")

def setup_cronjob(cron_schedule, pipeline_file):
    """Set up the cron job for the pipeline."""
    cron = CronTab(user=True)
    command = f"python {pipeline_file} --run"
    job = cron.new(command=command)
    job.setall(cron_schedule)
    cron.write()
    print(f"Cron job set up: {command} at schedule '{cron_schedule}'")

def list_cronjobs():
    """List all cron jobs for the current user."""
    cron = CronTab(user=True)
    print("Current cron jobs:")
    for job in cron:
        print(f"{job.scheduled_time()} - {job.command}")

def run_cli():
    """Command-line interface for running the pipeline."""
    parser = argparse.ArgumentParser(description="Pipeline CLI")
    parser.add_argument('--file', required=True, help="Path to the pipeline file")
    parser.add_argument('--run', action='store_true', help="Run the pipeline")
    parser.add_argument('--list-steps', action='store_true', help="List the steps in the pipeline")
    parser.add_argument('--check-errors', action='store_true', help="Check for errors in the log")
    parser.add_argument('--setup-cron', action='store_true', help="Set up the cron job for this pipeline")
    parser.add_argument('--list-cron', action='store_true', help="List all cron jobs")

    args = parser.parse_args()

    # Dynamically import the domain-specific pipeline file
    spec = importlib.util.spec_from_file_location("pipeline_module", args.file)
    pipeline_module = importlib.util.module_from_spec(spec)
    sys.modules["pipeline_module"] = pipeline_module
    spec.loader.exec_module(pipeline_module)

    if args.run:
        pipeline_module.dag.run()
    if args.list_steps:
        list_steps()
    if args.check_errors:
        check_errors('test.txt')
    if args.setup_cron:
        # If a cron_schedule is provided in the pipeline file, set up the cron job
        cron_schedule = getattr(pipeline_module, 'cron_schedule', None)
        if cron_schedule:
            setup_cronjob(cron_schedule, args.file)
        else:
            print("No cron schedule found in the pipeline file.")
    if args.list_cron:
        list_cronjobs()

if __name__ == "__main__":
    # run_cli()

    spec = importlib.util.spec_from_file_location("pipeline_module", "./src/my_pipeline.py")
    pipeline_module = importlib.util.module_from_spec(spec)
    sys.modules["pipeline_module"] = pipeline_module
    spec.loader.exec_module(pipeline_module)

    pipeline_module.dag.run()