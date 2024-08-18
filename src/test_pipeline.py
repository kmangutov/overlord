import pytest
from pipeline_lib import Pipeline, PipelineConfig, StepConfig, step

# Define test steps
@step()
def step1():
    return "Hello"

@step()
def step2(input_data):
    return f"{input_data}, World!"

def test_pipeline_execution():
    # Create a pipeline configuration
    pipeline_config = PipelineConfig(
        steps=[
            StepConfig(func=step1, name="Step 1"),
            StepConfig(func=step2, name="Step 2"),
        ]
    )
    
    # Create a pipeline
    pipeline = Pipeline(pipeline_config)
    
    # Run the pipeline
    result = pipeline.run()
    
    # Check if the final result is correct
    assert result == "Hello, World!"

def test_pipeline_empty():
    # Create an empty pipeline
    pipeline = Pipeline(PipelineConfig(steps=[]))
    
    # Attempt to run the empty pipeline
    with pytest.raises(ValueError, match="No steps to run in the Pipeline."):
        pipeline.run()

# Run the tests
if __name__ == "__main__":
    pytest.main([__file__])