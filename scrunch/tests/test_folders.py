from mock import MagicMock
from scrunch.folders import Folder
from scrunch.mutable_dataset import MutableDataset
from pycrunch.shoji import Entity, Catalog

from .test_datasets import AttributeDict
from .mock_session import MockSession


def _getitem(var):
    variables = {
        'foo': AttributeDict(url='http://example.foo/'),
        'bar': AttributeDict(url='http://example.bar/'),
    }
    return variables[var]


def test_move_here_simple():
    """ basic test assuring `Folder.move_here` doesn't throw a basestring
    exception '"""
    entity_mock = MagicMock()
    root_mock = MagicMock()
    root_mock.dataset.__getitem__.side_effect = _getitem

    folder = Folder(entity_mock, root_mock, MagicMock())
    folder.move_here(['foo', 'bar'])
    entity_mock.patch.assert_called()


def test_unique_folders():
    session = MockSession()
    dataset_url = 'http://host/api/datasets/abc/'
    folders_url = 'http://host/api/datasets/abc/folders/'
    public_url = 'http://host/api/datasets/abc/folders/public/'
    hidden_url = 'http://host/api/datasets/abc/folders/hidden/'
    secure_url = 'http://host/api/datasets/abc/folders/secure/'
    dataset_resource = Entity(session, **{
        "element": "shoji:entity",
        "self": dataset_url,
        "body": {
            "name": "test_dataset_project"
        },
        "catalogs": {
            "folders": folders_url,
        }
    })
    dataset_resource.variables = MagicMock()
    dataset_resource.settings = MagicMock()
    folders_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": folders_url,
        "index": {},
        "body": {
            "name": "Root"
        },
        "catalogs": {
            "public": public_url,
            "hidden": hidden_url,
            "secure": secure_url,
        }
    })
    public_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": public_url,
        "index": {},
        "body": {
            "name": "Public"
        },
    })
    hidden_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": hidden_url,
        "index": {},
        "body": {
            "name": "Hidden"
        },
    })
    secure_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": secure_url,
        "index": {},
        "body": {
            "name": "Secure"
        },
    })
    session.add_fixture(folders_url, folders_resource)
    session.add_fixture(public_url, public_resource)
    session.add_fixture(hidden_url, hidden_resource)
    session.add_fixture(secure_url, secure_resource)
    dataset = MutableDataset(dataset_resource)

    assert dataset.folders.public.name == "Public"
    assert dataset.folders.hidden.name == "Hidden"
    assert dataset.folders.secure.name == "Secure"


def test_legacy_without_public():
    session = MockSession()
    dataset_url = 'http://host/api/datasets/abc/'
    folders_url = 'http://host/api/datasets/abc/folders/'
    public_url = 'http://host/api/datasets/abc/folders/public/'
    dataset_resource = Entity(session, **{
        "element": "shoji:entity",
        "self": dataset_url,
        "body": {
            "name": "test_dataset_project"
        },
        "catalogs": {
            "folders": folders_url,
        }
    })
    dataset_resource.variables = MagicMock()
    dataset_resource.settings = MagicMock()

    folders_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": folders_url,
        "index": {},
        "catalogs": {
            "public": public_url
        }
    })
    public_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": public_url,
        "index": {},
        "body": {
            "name": "Root"
        },
        "catalogs": {
            "public": public_url
        }
    })
    session.add_fixture(folders_url, folders_resource)
    session.add_fixture(public_url, public_resource)
    dataset = MutableDataset(dataset_resource)

    assert dataset.folders.root.name == "Root"
    # Note how the public endpoint has Root because it uses the old API response
    assert dataset.folders.public.name == "Root"


def test_unique_folders_no_secure():
    session = MockSession()
    dataset_url = 'http://host/api/datasets/abc/'
    folders_url = 'http://host/api/datasets/abc/folders/'
    public_url = 'http://host/api/datasets/abc/folders/public/'
    hidden_url = 'http://host/api/datasets/abc/folders/hidden/'
    dataset_resource = Entity(session, **{
        "element": "shoji:entity",
        "self": dataset_url,
        "body": {
            "name": "test_dataset_project"
        },
        "catalogs": {
            "folders": folders_url,
        }
    })
    dataset_resource.variables = MagicMock()
    dataset_resource.settings = MagicMock()
    folders_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": folders_url,
        "index": {},
        "body": {
            "name": "Root"
        },
        "catalogs": {
            "public": public_url,
            "hidden": hidden_url,
            # Viewer users don't have the secure folder available
            # "secure": secure_url,
        }
    })
    public_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": hidden_url,
        "index": {},
        "body": {
            "name": "Public"
        },
    })
    hidden_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": hidden_url,
        "index": {},
        "body": {
            "name": "Hidden"
        },
    })
    session.add_fixture(folders_url, folders_resource)
    session.add_fixture(public_url, public_resource)
    session.add_fixture(hidden_url, hidden_resource)
    dataset = MutableDataset(dataset_resource)

    assert dataset.folders.public.name == "Public"
    assert dataset.folders.hidden.name == "Hidden"
    assert not hasattr(dataset.folders, "secure")


def test_unique_folders_no_hidden():
    session = MockSession()
    dataset_url = 'http://host/api/datasets/abc/'
    folders_url = 'http://host/api/datasets/abc/folders/'
    public_url = 'http://host/api/datasets/abc/folders/public/'
    dataset_resource = Entity(session, **{
        "element": "shoji:entity",
        "self": dataset_url,
        "body": {
            "name": "test_dataset_project"
        },
        "catalogs": {
            "folders": folders_url,
        }
    })
    dataset_resource.variables = MagicMock()
    dataset_resource.settings = MagicMock()
    folders_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": folders_url,
        "index": {},
        "body": {
            "name": "Root"
        },
        "catalogs": {
            # Standard exposed catalogs
            "public": public_url,
            "personal": "./personal/",
            "parents": "./parents/",
            # Viewer users don't have the secure folder available
            # "secure": secure_url,
            # Viewers also don't get the hidden folder exposed
            # "hidden": hidden_url,
        }
    })
    public_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": public_url,
        "index": {},
        "body": {
            "name": "Public"
        },
    })
    session.add_fixture(folders_url, folders_resource)
    session.add_fixture(public_url, public_resource)
    dataset = MutableDataset(dataset_resource)

    assert dataset.folders.public.name == "Public"
    assert not hasattr(dataset.folders, "secure")
    assert not hasattr(dataset.folders, "hidden")
