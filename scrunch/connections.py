# coding: utf-8

import os
import six
import logging
import pycrunch
from scrunch.session import connect
from scrunch.exceptions import AuthenticationError

if six.PY2:  # pragma: no cover
    import ConfigParser as configparser
else:
    import configparser


LOG = logging.getLogger('scrunch')


def _set_debug_log():
    # ref: http://docs.python-requests.org/en/master/api/#api-changes
    #
    #  These two lines enable debugging at httplib level
    # (requests->urllib3->http.client)
    # You will see the REQUEST, including HEADERS and DATA,
    # and RESPONSE with HEADERS but without DATA.
    # The only thing missing will be the response.body which is not logged.
    try:
        import http.client as http_client
    except ImportError:
        # Python 2
        import httplib as http_client
    http_client.HTTPConnection.debuglevel = 1
    LOG.setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def _get_connection(file_path='crunch.ini'):
    """
    Utilitarian function that reads credentials from
    file or from ENV variables
    """
    if pycrunch.session is not None:
        return pycrunch.session

    connection_kwargs = {}

    # try to get credentials from environment
    site = os.environ.get('CRUNCH_URL')
    if site:
        connection_kwargs["site_url"] = site

    api_key = os.environ.get('CRUNCH_API_KEY')
    username = os.environ.get('CRUNCH_USERNAME')
    password = os.environ.get('CRUNCH_PASSWORD')
    if api_key:
        connection_kwargs["api_key"] = api_key
    elif username and password:
        connection_kwargs["username"] = username
        connection_kwargs["pw"] = password

    if connection_kwargs:
        return connect(**connection_kwargs)

    # try reading from .ini file
    config = configparser.ConfigParser()
    config.read(file_path)
    try:
        site = config.get('DEFAULT', 'CRUNCH_URL')
    except Exception:
        pass  # Config not found in .ini file. Do not change env value
    else:
        connection_kwargs["site_url"] = site


    try:
        api_key = config.get('DEFAULT', 'CRUNCH_API_KEY')
    except Exception:
        pass  # Config not found in .ini file. Do not change env value
    else:
        connection_kwargs["api_key"] = api_key

    if not api_key:
        try:
            username = config.get('DEFAULT', 'CRUNCH_USERNAME')
            password = config.get('DEFAULT', 'CRUNCH_PASSWORD')
        except Exception:
            pass  # Config not found in .ini file. Do not change env value
        else:
            connection_kwargs["username"] = username
            connection_kwargs["pw"] = password

    # now try to login with obtained creds
    if connection_kwargs:
        return connect(**connection_kwargs)
    else:
        raise AuthenticationError(
            "Unable to find crunch session, crunch.ini file "
            "or environment variables.")


def _default_connection(connection):
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")
    return connection
