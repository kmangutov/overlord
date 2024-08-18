from pipeline_lib import step, Pipeline, PipelineConfig, StepConfig, sqlite_connection


@step()
def step1():
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
