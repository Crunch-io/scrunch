from scrunch.datasets import LOG, Dataset, _get_dataset


def get_mutable_dataset(dataset, connection=None, editor=False, project=None):
    """
    A simple wrapper of _get_dataset with streaming=False
    """
    LOG.warning("""MutableDataset is deprecated, instead use now
        Dataset with it's corresponding get_dataset() method""") # noqa: E501
    shoji_ds, root = _get_dataset(dataset, connection, editor, project)
    ds = Dataset(shoji_ds)
    if editor is True:
        ds.change_editor(root.session.email)
    return ds


class MutableDataset(Dataset):
    """
    Class that enclose mutable dataset methods or any
    method that varies the state of the dataset and/or it's data.
    """

    def __init__(self, resource):
        LOG.warning("""MutableDataset is deprecated, instead use now
            Dataset with it's corresponding get_dataset() method""") # noqa: E501
        super(Dataset, self).__init__(resource)
