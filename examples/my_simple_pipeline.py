"""
python3 src/pipeline_lib.py --file examples/my_simple_pipeline.py --run
python3 src/pipeline_lib.py --file examples/my_simple_pipeline.py --debug snapshots/step2_ZeroDivisionError.pkl
"""

from pipeline_lib import step, Pipeline, PipelineConfig, StepConfig


@step()
def step1():
    # Imagine this data comes from an API
    return 0

@step()
def step2(input_data, **kwargs):
    return 10 / (input_data + 5)  # This will raise a ZeroDivisionError when input_data is 0

@step()
def step3(input_data, **kwargs):
    print(f'step3 input_data = {input_data}')

# Create a pipeline configuration
pipeline_config = PipelineConfig(
    steps=[
        StepConfig(func=step1),
        StepConfig(func=step2),
        StepConfig(func=step3),
    ],
)

# Create a pipeline
pipeline = Pipeline(pipeline_config)
