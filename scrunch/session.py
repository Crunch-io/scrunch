from pycrunch.version import __version__ as pycrunch_version
from pycrunch import connect as _connect
from pycrunch.elements import ElementSession

from .version import __version__


class ScrunchSession(ElementSession):
    headers = {
        "user-agent": "scrunch/%s (pycrunch/%s)" % (__version__, pycrunch_version)
    }


def connect(*args, **kwargs):
    return _connect(session_class=ScrunchSession, *args, **kwargs)

