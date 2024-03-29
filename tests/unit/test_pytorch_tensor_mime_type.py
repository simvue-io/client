from simvue.serialization import Serializer, Deserializer
import torch

def test_pytorch_tensor_mime_type():
    """
    Check that a PyTorch tensor has the correct mime-type
    """
    torch.manual_seed(1724)
    array = torch.rand(2, 3)
    _, mime_type = Serializer().serialize(array)

    assert (mime_type == 'application/vnd.simvue.torch.v1')
