try:
    # py <= 3.7
    from importlib_metadata import version
except ModuleNotFoundError:
    # py >= 3.8
    from importlib.metadata import version


try:
    __version__ = version('scrunch')
except Exception:
    # Package is installed from source. It's at least this version
    __version__ = '0.18.4-uninstalled'
