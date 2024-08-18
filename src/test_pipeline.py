import pytest
from pipeline_lib import Pipeline, PipelineConfig, StepConfig, step

from unittest import mock


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
            StepConfig(func=step1),
            StepConfig(func=step2),
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

@mock.patch('pipeline_lib.snapshot_state')
def test_exception_handling_with_input(mock_snapshot_state):
    """
    Test that when an exception occurs in a pipeline step with input, the snapshot_state function
    is called with the correct parameters, including the step's input and the exception details.
    
    This test ensures that:
    1. The snapshot_state function is invoked when a step raises an exception.
    2. The snapshot includes the correct step name, input parameters, and exception information.
    3. The exception type and message are validated separately to handle object comparison issues.
    """
    
    @step()
    def step1():
        # Imagine this data comes from an API
        return 0
    
    @step()
    def step2(input_data):
        return 10 / input_data  # This will raise a ZeroDivisionError when input_data is 0
    
    # Create a pipeline configuration
    pipeline_config = PipelineConfig(
        steps=[
            StepConfig(func=step1),
            StepConfig(func=step2),
        ]
    )
    
    # Create a pipeline
    pipeline = Pipeline(pipeline_config)
    
    # Run the pipeline and expect a ZeroDivisionError
    with pytest.raises(ZeroDivisionError, match="division by zero"):
        pipeline.run()
    
    # Verify that snapshot_state was called with the correct parameters for the second step
    mock_snapshot_state.assert_called_with(
        'step2_ZeroDivisionError',
        state={
            'step_name': 'step2',
            'input': ((0,), {}),
            'exception': mock.ANY
        }
    )
    
    # Ensure the exception type and message are correct
    args, kwargs = mock_snapshot_state.call_args
    exception = kwargs['state']['exception']
    assert isinstance(exception, ZeroDivisionError)
    assert str(exception) == "division by zero"
    
    # Check if the snapshot_state was called exactly once
    assert mock_snapshot_state.call_count == 1


# Run the tests
if __name__ == "__main__":
    pytest.main([__file__])