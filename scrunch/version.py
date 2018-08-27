from pkg_resources import get_distribution, DistributionNotFound


try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # Package is installed from source. It's at least this version
    __version__ = '0.6.3-uninstalled'
