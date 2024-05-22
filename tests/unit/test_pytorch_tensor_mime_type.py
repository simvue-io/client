from simvue.serialization import serialize_object
import torch
import pytest

try:
    import torch
except ImportError:
    torch = None
    
@pytest.mark.skipif(not torch, reason="Torch is not installed")
def test_pytorch_tensor_mime_type():
    """
    Check that a PyTorch tensor has the correct mime-type
    """
    torch.manual_seed(1724)
    array = torch.rand(2, 3)
    _, mime_type = serialize_object(array, False)

    assert (mime_type == 'application/vnd.simvue.torch.v1')
