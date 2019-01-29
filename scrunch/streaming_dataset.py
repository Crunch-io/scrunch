import json

from pycrunch.importing import Importer
from scrunch.datasets import BaseDataset, _get_dataset
from scrunch.exceptions import InvalidDatasetTypeError
from scrunch.helpers import shoji_entity_wrapper

try:
    import pandas as pd
except ImportError:
    # pandas has not been installed, don't worry!
    # ... unless you have to worry about pandas
    pd = None


def get_streaming_dataset(dataset, connection=None, editor=False, project=None):
    """
    A simple wrapper of _get_dataset with streaming=True
    """
    shoji_ds, root = _get_dataset(dataset, connection, editor, project)
    # make sure the Dataset is of type streaming != "streaming"
    if shoji_ds['body'].get('streaming') != 'streaming':
        raise InvalidDatasetTypeError("Dataset %s is of type 'mutable',\
            use get_mutable_dataset method instead" % dataset)
    ds = StreamingDataset(shoji_ds)
    if editor is True:
        ds.change_editor(root.session.email)
    return ds


class StreamingDataset(BaseDataset):
    """
    A Crunch entity that represents Datasets that are currently
    of the "streaming" class
    """

    def stream_rows(self, columns):
        """
        Receives a dict with columns of values to add and streams them
        into the dataset. Client must call .push_rows(n) later or wait until
        Crunch automatically processes the batch.

        Returns the total of rows streamed
        """
        importer = Importer()
        count = len(list(columns.values())[0])
        for x in range(count):
            importer.stream_rows(self.resource,
                                 {a: columns[a][x] for a in columns})
        return count

    def push_rows(self, count=None):
        """
        Batches in the rows that have been recently streamed. This forces
        the rows to appear in the dataset instead of waiting for crunch
        automatic batcher process.
        """
        if bool(self.resource.stream.body.pending_messages):
            self.resource.batches.create(
                shoji_entity_wrapper({
                    'stream': count,
                    'type': 'ldjson'}
                ))

    def replace_from_csv(self, filename, chunksize=1000, push_rows=True):
        """
        Given a csv file in the format:
        id, var1_alias, var2_alias
        1,  14,         15

        where the first column is the Dataset PK

        Replace the values of the matching id, for the given variables
        in the Dataset using the /stream endpoint:

        [{id: 1, var1_alias: 14, var2_alias: 15}, ...]
        """
        importer = Importer()
        df_chunks = pd.read_csv(
            filename,
            header=0,
            chunksize=chunksize
        )
        for chunk in df_chunks:
            # This is a trick to get rid of np.int64, which is not
            # json serializable
            stream = chunk.to_json(orient='records')
            stream = json.loads(stream)
            importer.stream_rows(self.resource, stream)
            self.push_rows(chunksize)
