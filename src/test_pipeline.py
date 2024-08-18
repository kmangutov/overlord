import pytest
from pipeline_lib import Pipeline, step

# Define test steps
@step()
def step1():
    return "Hello"

@step()
def step2(input_data):
    return f"{input_data}, World!"
    

def test_pipeline_execution():
    # Create a pipeline
    pipeline = Pipeline()
    
    # Add steps to the pipeline
    pipeline << step1 << step2
    
    # Run the pipeline
    result = pipeline.run()
    
    # Check if the final result is correct
    assert result == "Hello, World!"


def test_pipeline_empty():
    # Create an empty pipeline
    pipeline = Pipeline()
    
    # Attempt to run the empty pipeline
    with pytest.raises(ValueError, match="No steps to run in the Pipeline."):
        pipeline.run()

# Run the tests
if __name__ == "__main__":
    pytest.main([__file__])