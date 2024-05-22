import torch
from simvue.serialization import serialize_object, deserialize_data
import pytest

try:
    import torch
except ImportError:
    torch = None

@pytest.mark.skipif(not torch, reason="Torch is not installed")
def test_pytorch_tensor_serialization():
    """
    Check that a PyTorch tensor can be serialized then deserialized successfully
    """
    torch.manual_seed(1724)
    array = torch.rand(2, 3)

    serialized, mime_type = serialize_object(array, False)
    array_out = deserialize_data(serialized, mime_type, False)

    assert (array == array_out).all()
