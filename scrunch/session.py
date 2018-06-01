from pycrunch import connect as _connect
from pycrunch.elements import ElementSession

from .version import __version__


class ScrunchSession(ElementSession):
    headers = {
        "user-agent": "scrunch/%s" % __version__
    }


def connect(*args, **kwargs):
    return _connect(session_class=ScrunchSession, *args, **kwargs)

