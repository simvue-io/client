from simvue import Run
import pytest

def test_run_init_tags():
    """
    Check that run.init throws an exception if tags are not a list
    """

    x1_lower = 2
    x1_upper = 6

    run = Run(mode='offline')

    with pytest.raises(RuntimeError) as exc_info:
        run.init(metadata={'dataset.x1_lower': x1_lower, 'dataset.x1_upper': x1_upper}, tags=1,
                description="A test to validate tag inputs passed into run.init",
                retention_period="1 hour"
        )

    assert "Input should be a valid list" in str(exc_info.value)
