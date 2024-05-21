from simvue.serialization import serialize_object, deserialize_data
import numpy as np

def test_numpy_array_serialization():
    """
    Check that a numpy array can be serialized then deserialized successfully
    """
    array = np.array([1, 2, 3, 4, 5])

    serialized, mime_type = serialize_object(array, False)
    array_out = deserialize_data(serialized, mime_type, False)

    assert (array == array_out).all()
