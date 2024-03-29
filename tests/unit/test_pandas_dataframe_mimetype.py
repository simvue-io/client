import pandas as pd
from simvue.serialization import Serializer, Deserializer

def test_pandas_dataframe_mimetype():
    """
    Check that the mime-type of a Pandas dataframe is correct
    """
    data = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=data)

    _, mime_type = Serializer().serialize(df)

    assert (mime_type == 'application/vnd.simvue.df.v1')
