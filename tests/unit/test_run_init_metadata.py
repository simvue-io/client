from simvue import Run
import pytest

def test_run_init_metadata():
    """
    Check that run.init throws an exception if tuples are passed into metadata dictionary
    """

    x1_lower = 2,
    x1_upper = 6,

    run = Run(mode='offline')

    with pytest.raises(RuntimeError) as exc_info:
        run.init(metadata={'dataset.x1_lower': x1_lower, 'dataset.x1_upper': x1_upper},
                description="A test to validate inputs passed into metadata dictionary",
                retention_period="1 hour"
        )

    assert "Input should be a valid integer" in str(exc_info.value)
