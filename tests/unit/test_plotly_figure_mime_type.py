from simvue.serialization import serialize_object
import matplotlib.pyplot as plt
import plotly
import pytest

try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

try:
    import plotly
except ImportError:
    plotly = None


@pytest.mark.skipif(not plt, reason="Matplotlib is not installed")
@pytest.mark.skipif(not plotly, reason="Plotly is not installed")
def test_plotly_figure_mime_type():
    """
    Check that a plotly figure has the correct mime-type
    """
    plt.plot([1, 2, 3, 4])
    figure = plt.gcf()
    plotly_figure = plotly.tools.mpl_to_plotly(figure)

    _, mime_type = serialize_object(plotly_figure, False)

    assert (mime_type == 'application/vnd.plotly.v1+json')
