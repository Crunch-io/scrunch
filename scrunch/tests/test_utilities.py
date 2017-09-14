import pytest
import mock

import scrunch
from scrunch.variables import validate_variable_url
from scrunch import get_project, get_mutable_dataset, get_user
from scrunch.datasets import Project, User
from scrunch.mutable_dataset import MutableDataset


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


def _by_side_effect(shoji, entity_mock):
    d = {
        'id': {shoji['body']['id']: entity_mock},
        'name': {shoji['body']['name']: entity_mock},
    }

    def _get(*args):
        return d.get(args[0])
    return _get


class TestUtilities(object):

    @mock.patch('pycrunch.session')
    def test_get_connection_with_session(self, session_mock):
        assert scrunch.datasets._get_connection() == session_mock

    @mock.patch('pycrunch.connect')
    def test_get_connection_with_ini(self, connect_mock):
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ini_file = os.path.join(current_dir, 'crunch_test.ini')
        scrunch.datasets._get_connection(ini_file)
        user = 'testuser@yougov.com'
        pw = 'supersecret'
        site = 'https://test.crunch.io/api/'
        assert connect_mock.call_args[0] == (user, pw, site)

    @mock.patch('pycrunch.connect')
    def test_get_connection_with_env(self, connect_mock, envpatch):
        import os
        user = os.environ.get('CRUNCH_USERNAME')
        pw = os.environ.get('CRUNCH_PASSWORD')
        assert user == 'USERNAME'
        assert pw == 'PASSWORD'
        scrunch.datasets._get_connection()
        assert connect_mock.call_args[0] == (user, pw)

    @mock.patch('pycrunch.session')
    def test_get_dataset(self, session):
        shoji_entity = {
            'element': 'shoji:entity',
            'body': {
                'id': '123456',
                'name': 'dataset_name',
                'streaming': 'no'
            }
        }

        ds_mock = mock.MagicMock(**shoji_entity)
        ds_mock.entity = mock.MagicMock(**shoji_entity)
        session.datasets.by.side_effect = _by_side_effect(shoji_entity, ds_mock)

        ds = get_mutable_dataset('dataset_name')
        session.datasets.by.assert_called_with('name')
        assert isinstance(ds, MutableDataset)
        assert ds.name == 'dataset_name'

    @mock.patch('pycrunch.session')
    def test_get_dataset_from_project_no_name(self, session):
        shoji_entity = {
            'element': 'shoji:entity',
            'body': {
                'id': '123456',
                'name': 'dataset_name',
                'streaming': 'no'
            }
        }

        ds_res = mock.MagicMock(**shoji_entity)
        ds_res.entity = mock.MagicMock(**shoji_entity)
        session.datasets.by.side_effect = KeyError()

        response = mock.MagicMock()
        response.payload = ds_res

        def _get(*args, **kwargs):
            return response

        session.session.get.side_effect = _get
        session.catalogs.datasets = 'https://test.crunch.io/api/'

        ds = get_mutable_dataset('123456')
        session.session.get.assert_called_with('https://test.crunch.io/api/123456/')

        assert isinstance(ds, MutableDataset)
        assert ds.name == 'dataset_name'
        assert ds.id == '123456'

    @mock.patch('pycrunch.session')
    def test_get_project(self, session):

        shoji_entity = {
            "element": "shoji:catalog",
            "body": {
                "name": "Y Team",
                "id": "614a7b2ebe9a4292bba54edce83563ae"
            }
        }

        site_mock = mock.MagicMock(**shoji_entity)
        site_mock.entity = mock.MagicMock(**shoji_entity)
        session.projects.by.side_effect = _by_side_effect(shoji_entity, site_mock)

        project = get_project('Y Team')
        session.projects.by.assert_called_with('name')
        assert isinstance(project, Project)
        assert project.id == '614a7b2ebe9a4292bba54edce83563ae'

    @mock.patch('pycrunch.session')
    def test_get_project_by_id(self, session):

        shoji_entity = {
            "element": "shoji:catalog",
            "body": {
                "name": "Y Team",
                "id": "614a7b2ebe9a4292bba54edce83563ae"
            }
        }

        site_mock = mock.MagicMock(**shoji_entity)
        site_mock.entity = mock.MagicMock(**shoji_entity)
        session.projects.by.side_effect = _by_side_effect(shoji_entity, site_mock)

        project = get_project('614a7b2ebe9a4292bba54edce83563ae')
        session.projects.by.assert_called_with('id')
        assert isinstance(project, Project)

        with pytest.raises(KeyError, message='Project invalidid not found.'):
            get_project('invalidid')

    @mock.patch('pycrunch.session')
    def test_get_user(self, session):

        shoji_entity = {
            "element": "shoji:catalog",
            "body": {
                "name": "Heisenberg",
                "email": "heisenberg@sc.org",
            }
        }

        site_mock = mock.MagicMock(**shoji_entity)
        site_mock.entity = mock.MagicMock(**shoji_entity)
        session.users.by.side_effect = {"heisenberg@sc.org": site_mock},

        user = get_user('heisenberg@sc.org')
        session.users.by.assert_called_with('email')
        assert isinstance(user, User)
