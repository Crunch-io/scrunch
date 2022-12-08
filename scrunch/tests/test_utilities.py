#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import mock
from pycrunch import ClientError, shoji, lemonpy

import scrunch
from scrunch.variables import validate_variable_url
from scrunch import get_project, get_mutable_dataset, get_user
from scrunch.datasets import Project, User
from scrunch.mutable_dataset import MutableDataset
from scrunch.order import Path
from scrunch.exceptions import InvalidPathError

from .mock_session import MockSession


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


def mocked_project_get_child(name):
    session = MockSession()
    session.feature_flags = {'old_projects_order': False}
    res_url = 'http://example.com/api/projects/A/'
    payload = {
        'element': 'shoji:entity',
        'self': res_url,
        'catalogs': {},
        'body': {'name': name, 'id': 1},
        'index': {},
        'graph': []
    }
    session.add_fixture(res_url, payload)

    return Project(session.get(res_url).payload)


class TestUtilities(object):

    @mock.patch('pycrunch.session')
    def test_get_connection_with_session(self, session_mock):
        assert scrunch.connections._get_connection() == session_mock

    @mock.patch('scrunch.connections.connect')
    def test_get_connection_with_ini(self, connect_mock):
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ini_file = os.path.join(current_dir, 'crunch_test.ini')
        scrunch.connections._get_connection(ini_file)
        user = 'testuser@yougov.com'
        pw = 'supersecret'
        site = 'https://test.crunch.io/api/'
        assert connect_mock.mock_calls[0].kwargs == {'site_url': site, 'username': user, 'pw': pw}

    @mock.patch('scrunch.connections.connect')
    def test_get_connection_with_ini_api_key(self, connect_mock):
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        ini_file = os.path.join(current_dir, 'crunch_test_api_key.ini')
        scrunch.connections._get_connection(ini_file)
        api_key = 'apikeysupersecret'
        site = 'https://test.crunch.io/api/'
        assert connect_mock.mock_calls[0].kwargs == {'site_url': site, 'api_key': api_key}

    @mock.patch('scrunch.connections.connect')
    def test_get_connection_with_env(self, connect_mock, envpatch):
        import os
        user = os.environ.get('CRUNCH_USERNAME')
        pw = os.environ.get('CRUNCH_PASSWORD')
        assert user == 'USERNAME'
        assert pw == 'PASSWORD'
        scrunch.datasets._get_connection()
        assert connect_mock.mock_calls[0].kwargs == {'username': user, 'pw': pw}

    @mock.patch('pycrunch.session')
    def test_get_dataset(self, root):
        session = root.session
        dataset_url = "/api/datasets/123456/"
        shoji_entity = {
            'element': 'shoji:entity',
            'body': {
                'id': '123456',
                'name': 'dataset_name',
                'streaming': 'no'
            },
            'catalogs': {
                "variables": "variables/"
            }
        }

        def _get(url, *args, **kwargs):
            if url == '/api/datasets/dataset_name/':
                # This is the first attempt to get by ID, which should fail
                resp = mock.MagicMock(status_code=404)
                raise ClientError(resp)
            elif url == dataset_url:
                # This is when we GET by the correct URL that has been
                # discovered by using `datasets_by_name`
                payload = mock.MagicMock(**shoji_entity)
                payload.self = dataset_url
                return mock.MagicMock(payload=payload)

        session.get.side_effect = _get

        root.catalogs.datasets = '/api/datasets/'
        catalog_url = lemonpy.URL("/api/datasets/by_name/", None)

        # Calling root.follow should return the "by_name" catalog
        by_name_catalog = shoji.Catalog(session, **{
            "index": shoji.Index(session, catalog_url, **{
                dataset_url: shoji.Tuple(session, dataset_url, **{
                    "name": "dataset_name"
                })
            })
        })
        root.follow.return_value = by_name_catalog
        ds = get_mutable_dataset('dataset_name')

        # We called indeed the `datasets_by_name` with the right arg
        root.follow.assert_called_with("datasets_by_name", {
            "name": "dataset_name"
        })

        assert isinstance(ds, MutableDataset)
        assert ds.name == 'dataset_name'
        assert ds.url == dataset_url

    @mock.patch('pycrunch.session')
    def test_get_dataset_from_project_no_name(self, session):
        dataset_id = 'b2c4c6b7d3a94e58937b23c1fed1b65e'
        shoji_entity = {
            'element': 'shoji:entity',
            'body': {
                'id': dataset_id,
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

        ds = get_mutable_dataset(dataset_id)
        session.session.get.assert_called_with('https://test.crunch.io/api/b2c4c6b7d3a94e58937b23c1fed1b65e/')

        assert isinstance(ds, MutableDataset)
        assert ds.name == 'dataset_name'
        assert ds.id == dataset_id

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
    @mock.patch('scrunch.datasets.Project.get_child', side_effect=mocked_project_get_child)
    def test_get_project_nested(self, mocked_get_child, session):

        shoji_entity = {
            "element": "shoji:catalog",
            "body": {
                "name": "Y Team",
                "id": "614a7b2ebe9a4292bba54edce83563ae"
            },
            "index": {
                "9165e5f4eb004bb4b257a90645bfb968": {
                    "name": "California",
                    "id": "9165e5f4eb004bb4b257a90645bfb968",
                    "type": "project"
                }
            }
        }

        site_mock = mock.MagicMock(**shoji_entity)
        site_mock.entity = mock.MagicMock(**shoji_entity)
        session.projects.by.side_effect = _by_side_effect(shoji_entity, site_mock)

        project = get_project('Y Team|California')
        assert isinstance(project, Project)
        assert str(project) == 'California'
        mocked_get_child.assert_called_with('California')

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

        with pytest.raises(KeyError) as excinfo:
            get_project('invalidid')
        assert str(excinfo.value) == \
               "'Project (name or id: invalidid) not found.'"
        # ^ That exception message is wrapped in quotes? Ugh?

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

    def test_path(self):
        with pytest.raises(TypeError) as excinfo:
            Path(1234)
        assert str(excinfo.value) == 'The path must be a string object'

        Path('|If Skadeförsäkring')
        Path('|æøå')

        path = Path('test')
        assert path.is_relative is True
        assert repr(path) == 'test'

    def test_user_agent(self):
        user = 'testuser@yougov.com'
        pw = 'supersecret'
        site = 'https://test.crunch.io/api/'
        from pycrunch.version import __version__ as pycrunch_v
        from requests.sessions import Session
        with mock.patch.object(Session, 'send') as mock_send:
            scrunch.connect(user, pw, site)
        prep_req = mock_send.call_args[0][0]
        assert prep_req.headers['user-agent'] == 'scrunch/%s (pycrunch/%s)' % (scrunch.__version__, pycrunch_v)
