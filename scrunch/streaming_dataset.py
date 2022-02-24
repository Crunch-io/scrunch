from pycrunch.importing import Importer
from scrunch.datasets import BaseDataset, _get_dataset
from scrunch.exceptions import InvalidDatasetTypeError
from scrunch.helpers import shoji_entity_wrapper


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
        authenticated_url = root.urls["user_url"]
        ds.change_editor(authenticated_url)
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
