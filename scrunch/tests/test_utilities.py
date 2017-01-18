import pytest
import mock

import scrunch
from scrunch.variables import validate_variable_url
# from scrunch.datasets import Dataset


@pytest.fixture(scope='function')
def envpatch():
    import os
    os.environ['CRUNCH_USERNAME'] = 'USERNAME'
    os.environ['CRUNCH_PASSWORD'] = 'PASSWORD'


def test_variable_url_validation():
    ds_url = 'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3/'
    assert not validate_variable_url(ds_url)

    var_url = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3/'
    )
    assert validate_variable_url(var_url)

    var_wo_trailing_slash = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3'
    )
    assert validate_variable_url(var_wo_trailing_slash)

    subvar_url = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3'
        '/subvariables/b4d10b49c385aa405756fbbf572649d3/'
    )
    assert validate_variable_url(subvar_url)

    subvar_wo_trailing_slash = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3'
        '/subvariables/b4d10b49c385aa405756fbbf572649d3'
    )
    assert validate_variable_url(subvar_wo_trailing_slash)

    var_catalog = (
        'https://test.crunch.io/api/datasets/b4d10b49c385aa405756fbbf572649d3'
        '/variables/b4d10b49c385aa405756fbbf572649d3/summary'
    )
    assert not validate_variable_url(var_catalog)


class TestGetDataset(object):

    @mock.patch('pycrunch.session')
    def test_get_connection_with_session(self, session_mock):
        assert scrunch.datasets._get_connection() == session_mock
        # import pdb; pdb.set_trace()

    @mock.patch('pycrunch.connect')
    def test_get_connection_with_env(self, connect_mock, envpatch):
        import os
        user = os.environ.get('CRUNCH_USERNAME')
        pw = os.environ.get('CRUNCH_PASSWORD')
        assert user == 'USERNAME'
        assert pw == 'PASSWORD'

        scrunch.datasets._get_connection()
        assert connect_mock.call_args[0] == (user, pw)
