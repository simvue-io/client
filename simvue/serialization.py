from io import BytesIO
import pickle


from .utilities import check_extra

class Serializer:
    def serialize(self, data, allow_pickle=False):
        serializer = get_serializer(data, allow_pickle)
        if serializer:
            return serializer(data)
        return None, None

def _is_torch_tensor(data):
    """
    Check if a dictionary is a PyTorch tensor or state dict
    """
    module_name = data.__class__.__module__
    class_name = data.__class__.__name__

    if module_name == 'collections' and class_name == 'OrderedDict':
        valid = True
        for item in data:
            module_name = data[item].__class__.__module__
            class_name = data[item].__class__.__name__
            if module_name != 'torch' or class_name != 'Tensor':
                valid = False
        if valid:
            return True
    elif module_name == 'torch' and class_name == 'Tensor':
        return True

    return False

def get_serializer(data, allow_pickle):
    """
    Determine which serializer to use
    """
    module_name = data.__class__.__module__
    class_name = data.__class__.__name__

    if module_name == 'plotly.graph_objs._figure' and class_name == 'Figure':
        return _serialize_plotly_figure
    elif module_name == 'matplotlib.figure' and class_name == 'Figure':
        return _serialize_matplotlib_figure
    elif module_name == 'numpy' and class_name == 'ndarray':
        return _serialize_numpy_array
    elif module_name == 'pandas.core.frame' and class_name == 'DataFrame':
        return _serialize_dataframe
    elif _is_torch_tensor(data):
        return _serialize_torch_tensor
    elif allow_pickle:
        return _serialize_pickle
    return None

@check_extra("plot")
def _serialize_plotly_figure(data):
    try:
        import plotly
    except ImportError:
        return
    mimetype = 'application/vnd.plotly.v1+json'
    data = plotly.io.to_json(data, 'json')
    return data, mimetype

@check_extra("plot")
def _serialize_matplotlib_figure(data):
    try:
        import plotly
    except ImportError:
        return None
    mimetype = 'application/vnd.plotly.v1+json'
    data = plotly.io.to_json(plotly.tools.mpl_to_plotly(data), 'json')
    return data, mimetype

@check_extra("dataset")
def _serialize_numpy_array(data):
    try:
        import numpy as np
    except ImportError:
        np = None
        return None

    mimetype = 'application/vnd.simvue.numpy.v1'
    mfile = BytesIO()
    np.save(mfile, data, allow_pickle=False)
    mfile.seek(0)
    data = mfile.read()
    return data, mimetype

@check_extra("dataset")
def _serialize_dataframe(data):
    mimetype = 'application/vnd.simvue.df.v1'
    mfile = BytesIO()
    data.to_csv(mfile)
    mfile.seek(0)
    data = mfile.read()
    return data, mimetype

@check_extra("torch")
def _serialize_torch_tensor(data):
    try:
        import torch
    except ImportError:
        torch = None
        return None

    mimetype = 'application/vnd.simvue.torch.v1'
    mfile = BytesIO()
    torch.save(data, mfile)
    mfile.seek(0)
    data = mfile.read()
    return data, mimetype

def _serialize_pickle(data):
    mimetype = 'application/octet-stream'
    data = pickle.dumps(data)
    return data, mimetype

class Deserializer:
    def deserialize(self, data, mimetype, allow_pickle=False):
        deserializer = get_deserializer(mimetype, allow_pickle)
        if deserializer:
            return deserializer(data)
        return None

def get_deserializer(mimetype, allow_pickle):
    """
    Determine which deserializer to use
    """
    if mimetype == 'application/vnd.plotly.v1+json':
        return _deserialize_plotly_figure
    elif mimetype == 'application/vnd.plotly.v1+json':
        return _deserialize_matplotlib_figure
    elif mimetype == 'application/vnd.simvue.numpy.v1':
        return _deserialize_numpy_array
    elif mimetype == 'application/vnd.simvue.df.v1':
        return _deserialize_dataframe
    elif mimetype == 'application/vnd.simvue.torch.v1':
        return _deserialize_torch_tensor
    elif mimetype == 'application/octet-stream' and allow_pickle:
        return _deserialize_pickle
    return None

@check_extra("plot")
def _deserialize_plotly_figure(data):
    try:
        import plotly
    except ImportError:
        return None
    data = plotly.io.from_json(data)
    return data

@check_extra("plot")
def _deserialize_matplotlib_figure(data):
    try:
        import plotly
    except ImportError:
        return None
    data = plotly.io.from_json(data)
    return data

@check_extra("dataset")
def _deserialize_numpy_array(data):
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
def _deserialize_dataframe(data):
    try:
        import pandas as pd
    except ImportError:
        pd = None
        return None

    mfile = BytesIO(data)
    mfile.seek(0)
    data = pd.read_csv(mfile, index_col=0)
    return data

@check_extra("torch")
def _deserialize_torch_tensor(data):
    try:
        import torch
    except ImportError:
        torch = None
        return None

    mfile = BytesIO(data)
    mfile.seek(0)
    data = torch.load(mfile)
    return data

def _deserialize_pickle(data):
    data = pickle.loads(data)
    return data
