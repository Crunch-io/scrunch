from pycrunch import connect

from .datasets import get_user, get_project, get_dataset
from .streaming_dataset import get_streaming_dataset
from .mutable_dataset import get_mutable_dataset, create_dataset

from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass
