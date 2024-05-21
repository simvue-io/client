"""
Object Serialization
====================

Contains serializers for storage of objects on the Simvue server
"""

import typing
import pickle
from io import BytesIO

if typing.TYPE_CHECKING:
    from pandas import DataFrame
    from plotly.graph_objects import Figure
    from torch import Tensor
    from typing_extensions import Buffer
    from .types import DeserializedContent

from .utilities import check_extra


def _is_torch_tensor(data: typing.Any) -> bool:
    """
    Check if value is a PyTorch tensor or state dict
    """
    module_name = data.__class__.__module__
    class_name = data.__class__.__name__

    if module_name == "collections" and class_name == "OrderedDict":
        valid = True
        for item in data:
            module_name = data[item].__class__.__module__
            class_name = data[item].__class__.__name__
            if module_name != "torch" or class_name != "Tensor":
                valid = False
        if valid:
            return True
    elif module_name == "torch" and class_name == "Tensor":
        return True

    return False


def serialize_object(
    data: typing.Any, allow_pickle: bool
) -> typing.Optional[tuple[str, str]]:
    """Determine which serializer to use for the given object

    Parameters
    ----------
    data : typing.Any
        object to serialize
    allow_pickle : bool
        whether pickling is allowed

    Returns
    -------
    Callable[[typing.Any], tuple[str, str]]
        the serializer to user
    """
    module_name = data.__class__.__module__
    class_name = data.__class__.__name__

    if module_name == "plotly.graph_objs._figure" and class_name == "Figure":
        return _serialize_plotly_figure(data)
    elif module_name == "matplotlib.figure" and class_name == "Figure":
        return _serialize_matplotlib_figure(data)
    elif module_name == "numpy" and class_name == "ndarray":
        return _serialize_numpy_array(data)
    elif module_name == "pandas.core.frame" and class_name == "DataFrame":
        return _serialize_dataframe(data)
    elif _is_torch_tensor(data):
        return _serialize_torch_tensor(data)
    elif module_name == "builtins" and class_name == "module" and not allow_pickle:
        try:
            import matplotlib.pyplot

            if data == matplotlib.pyplot:
                return _serialize_matplotlib(data)
        except ImportError:
            pass

    if allow_pickle:
        return _serialize_pickle(data)
    return None


@check_extra("plot")
def _serialize_plotly_figure(data: typing.Any) -> typing.Optional[tuple[str, str]]:
    try:
        import plotly
    except ImportError:
        return None
    mimetype = "application/vnd.plotly.v1+json"
    data = plotly.io.to_json(data, "json")
    return data, mimetype


@check_extra("plot")
def _serialize_matplotlib(data: typing.Any) -> typing.Optional[tuple[str, str]]:
    try:
        import plotly
    except ImportError:
        return None
    mimetype = "application/vnd.plotly.v1+json"
    data = plotly.io.to_json(plotly.tools.mpl_to_plotly(data.gcf()), "json")
    return data, mimetype


@check_extra("plot")
def _serialize_matplotlib_figure(data: typing.Any) -> typing.Optional[tuple[str, str]]:
    try:
        import plotly
    except ImportError:
        return None
    mimetype = "application/vnd.plotly.v1+json"
    data = plotly.io.to_json(plotly.tools.mpl_to_plotly(data), "json")
    return data, mimetype


@check_extra("dataset")
def _serialize_numpy_array(data: typing.Any) -> typing.Optional[tuple[str, str]]:
    try:
        import numpy as np
    except ImportError:
        np = None
        return None

    mimetype = "application/vnd.simvue.numpy.v1"
    mfile = BytesIO()
    np.save(mfile, data, allow_pickle=False)
    mfile.seek(0)
    data = mfile.read()
    return data, mimetype


@check_extra("dataset")
def _serialize_dataframe(data: typing.Any) -> typing.Optional[tuple[str, str]]:
    mimetype = "application/vnd.simvue.df.v1"
    mfile = BytesIO()
    data.to_csv(mfile)
    mfile.seek(0)
    data = mfile.read()
    return data, mimetype


@check_extra("torch")
def _serialize_torch_tensor(data: typing.Any) -> typing.Optional[tuple[str, str]]:
    try:
        import torch
    except ImportError:
        torch = None
        return None

    mimetype = "application/vnd.simvue.torch.v1"
    mfile = BytesIO()
    torch.save(data, mfile)
    mfile.seek(0)
    data = mfile.read()
    return data, mimetype


def _serialize_pickle(data: typing.Any) -> typing.Optional[tuple[str, str]]:
    mimetype = "application/octet-stream"
    data = pickle.dumps(data)
    return data, mimetype


def deserialize_data(
    data: "Buffer", mimetype: str, allow_pickle: bool
) -> typing.Optional["DeserializedContent"]:
    """
    Determine which deserializer to use
    """
    if mimetype == "application/vnd.plotly.v1+json":
        return _deserialize_plotly_figure(data)
    elif mimetype == "application/vnd.plotly.v1+json":
        return _deserialize_matplotlib_figure(data)
    elif mimetype == "application/vnd.simvue.numpy.v1":
        return _deserialize_numpy_array(data)
    elif mimetype == "application/vnd.simvue.df.v1":
        return _deserialize_dataframe(data)
    elif mimetype == "application/vnd.simvue.torch.v1":
        return _deserialize_torch_tensor(data)
    elif mimetype == "application/octet-stream" and allow_pickle:
        return _deserialize_pickle(data)
    return None


@check_extra("plot")
def _deserialize_plotly_figure(data: "Buffer") -> typing.Optional["Figure"]:
    try:
        import plotly
    except ImportError:
        return None
    data = plotly.io.from_json(data)
    return data


@check_extra("plot")
def _deserialize_matplotlib_figure(data: "Buffer") -> typing.Optional["Figure"]:
    try:
        import plotly
    except ImportError:
        return None
    data = plotly.io.from_json(data)
    return data


@check_extra("dataset")
def _deserialize_numpy_array(data: "Buffer") -> typing.Optional[typing.Any]:
    try:
        import numpy as np
    except ImportError:
        np = None
        return None

    mfile = BytesIO(data)
    mfile.seek(0)
    data = np.load(mfile, allow_pickle=False)
    return data


@check_extra("dataset")
def _deserialize_dataframe(data: "Buffer") -> typing.Optional["DataFrame"]:
    try:
        import pandas as pd
    except ImportError:
        pd = None
        return None

    mfile = BytesIO(data)
    mfile.seek(0)
    return pd.read_csv(mfile, index_col=0)


@check_extra("torch")
def _deserialize_torch_tensor(data: "Buffer") -> typing.Optional["Tensor"]:
    try:
        import torch
    except ImportError:
        torch = None
        return None

    mfile = BytesIO(data)
    mfile.seek(0)
    return torch.load(mfile)


def _deserialize_pickle(data):
    data = pickle.loads(data)
    return data
