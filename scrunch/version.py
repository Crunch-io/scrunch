from importlib_metadata import version


try:
    __version__ = version('scrunch')
except Exception:
    # Package is installed from source. It's at least this version
    __version__ = '0.15.3-uninstalled'
