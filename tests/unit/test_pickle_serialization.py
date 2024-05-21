from simvue.serialization import deserialize_data, serialize_object

def test_pickle_serialization():
    """
    Check that a dictionary can be serialized then deserialized successfully
    """
    data = {'a': 1.0, 'b': 'test'}

    serialized, mime_type = serialize_object(data, allow_pickle=True)
    data_out = deserialize_data(serialized, mime_type, allow_pickle=True)

    assert (data == data_out)
