import pytest
from simvue.serialization import serialize_object
import matplotlib.pyplot as plt

try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

@pytest.mark.skipif(not plt, reason="Matplotlib is not installed")
def test_matplotlib_figure_mime_type():
    """
    Check that a matplotlib figure has the correct mime-type
    """
    plt.plot([1, 2, 3, 4])
    figure = plt.gcf()

    _, mime_type = serialize_object(figure, False)

    assert (mime_type == 'application/vnd.plotly.v1+json')
