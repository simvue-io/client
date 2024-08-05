from simvue import Run
import pytest
import logging

def test_suppress_errors_false():
    """
    Check that exceptions are thrown if suppress_errors disabled
    """
    run = Run()

    with pytest.raises(RuntimeError) as e:
        run.config(
            suppress_errors=False,
            disable_resources_metrics=123,
        )
    assert "Input should be a valid boolean, unable to interpret input" in f"{e.value}"
        
def test_suppress_errors_true(caplog):
    """
    Check that no exceptions are thrown and messages are added to log if suppress_errors enabled
    """
    run = Run()

    run.config(suppress_errors=True)
    run.config(
        disable_resources_metrics=123,
    )
    
    caplog.set_level(logging.ERROR)
    
    assert "Input should be a valid boolean, unable to interpret input" in caplog.text

def test_suppress_errors_default(caplog):
    """
    Check that by default no exceptions are thrown and messages are added to log
    """
    run = Run()

    run.config(suppress_errors=True)
    run.config(
        disable_resources_metrics=123,
    )
    
    caplog.set_level(logging.ERROR)
    
    assert "Input should be a valid boolean, unable to interpret input" in caplog.text
