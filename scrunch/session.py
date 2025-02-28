import os

from pycrunch import connect as _connect
from pycrunch.elements import ElementSession
from pycrunch.version import __version__ as pycrunch_version

from .version import __version__

SSL_UNSAFE = os.environ.get("SSL_UNSAFE", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
)


class ScrunchSession(ElementSession):
    headers = {
        "user-agent": "scrunch/%s (pycrunch/%s)" % (__version__, pycrunch_version)
    }


class ScrunchSSLUnsafeSession(ScrunchSession):
    """
    A subclass of `ScrunchSession` that skips SSL certificate validation
    when trying to connect to the API server. Useful for local testing.
    """

    def __init__(self, *args, **kwargs):
        super(ScrunchSSLUnsafeSession, self).__init__(*args, **kwargs)
        self.verify = False


FLAGS_TO_CHECK = {"old_projects_order", "clients_strict_subvariable_syntax"}


def set_feature_flags(site):
    feature_flags = {
        flag_name: site.follow("feature_flag", "feature_name=%s" % flag_name).value[
            "active"
        ]
        for flag_name in FLAGS_TO_CHECK
    }
    setattr(site.session, "feature_flags", feature_flags)
    return site


def connect(*args, **kwargs):
    session_class = ScrunchSSLUnsafeSession if SSL_UNSAFE else ScrunchSession
    _site = _connect(session_class=session_class, *args, **kwargs)
    _site = set_feature_flags(_site)
    return _site
