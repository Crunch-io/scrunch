from scrunch.datasets import LOG, Dataset, _get_dataset


def get_streaming_dataset(dataset, connection=None, editor=False, project=None):
    """
    A simple wrapper of _get_dataset with streaming=True
    """
    LOG.warning("""StreamingDataset is deprecated, instead use now
        Dataset with it's corresponding get_dataset() method""")  # noqa: E501
    shoji_ds, root = _get_dataset(dataset, connection, editor, project)
    ds = Dataset(shoji_ds)
    if editor is True:
        ds.change_editor(root.session.email)
    return ds


class StreamingDataset(Dataset):
    """
    A Crunch entity that represents Datasets that are currently
    of the "streaming" class
    """

    def __init__(self, resource):
        LOG.warning("""StreamingDataset is deprecated, instead use now
            Dataset with it's corresponding get_dataset() method""")  # noqa: E501
        super(Dataset, self).__init__(resource)
