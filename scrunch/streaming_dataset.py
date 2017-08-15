from scrunch.datasets import BaseDataset, _get_dataset
from scrunch.exceptions import InvalidDatasetTypeError


def get_streaming_dataset(dataset, connection=None, editor=False):
    """
    A simple wrapper of _get_dataset with streaming=True
    """
    shoji_ds, root = _get_dataset(dataset, connection, editor, streaming=True)
    # make sure the Dataset is of type streaming != "streaming"
    if shoji_ds['body'].get('streaming') == 'streaming':
        raise InvalidDatasetTypeError("Dataset %s is of type 'streaming',\
            use get_streaming_dataset method instead" % dataset)
    ds = StreamingDataset(shoji_ds)
    if editor is True:
        ds.change_editor(root.session.email)
    return ds


class StreamingDataset(BaseDataset):
    """
    A Crunch entity that represents Datasets that are currently
    of the "streaming" class
    """
    pass
