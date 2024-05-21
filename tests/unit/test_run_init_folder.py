from simvue import Run
import pytest


def test_run_init_folder():
    """
    Check that run.init throws an exception if folder input is not specified correctly
    """
    x1_lower = 2
    x1_upper = 6

    run = Run(mode="offline")

    with pytest.raises(RuntimeError) as exc_info:
        run.init(
            metadata={"dataset.x1_lower": x1_lower, "dataset.x1_upper": x1_upper},
            tags=["tag_1", "tag_2"],
            folder="test_folder",
            description="A test to validate folder input passed into run.init",
            retention_period="1 hour",
        )
    assert "String should match pattern" in str(exc_info.value)
