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
    hidden_url = 'http://host/api/datasets/abc/folders/hidden/'
    secure_url = 'http://host/api/datasets/abc/folders/secure/'
    trash_url = 'http://host/api/datasets/abc/folders/trash/'
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
            "hidden": hidden_url,
            "secure": secure_url,
            "trash": trash_url,
        }
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
    trash_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": trash_url,
        "index": {},
        "body": {
            "name": "Trash"
        },
    })
    session.add_fixture(folders_url, folders_resource)
    session.add_fixture(hidden_url, hidden_resource)
    session.add_fixture(secure_url, secure_resource)
    session.add_fixture(trash_url, trash_resource)
    dataset = MutableDataset(dataset_resource)

    assert dataset.folders.root.name == "Root"
    assert dataset.folders.hidden.name == "Hidden"
    assert dataset.folders.secure.name == "Secure"
    assert dataset.folders.trash.name == "Trash"


def test_unique_folders_no_secure():
    session = MockSession()
    dataset_url = 'http://host/api/datasets/abc/'
    folders_url = 'http://host/api/datasets/abc/folders/'
    hidden_url = 'http://host/api/datasets/abc/folders/hidden/'
    trash_url = 'http://host/api/datasets/abc/folders/trash/'
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
            "hidden": hidden_url,
            # Viewer users don't have the secure folder available
            # "secure": secure_url,
            "trash": trash_url,
        }
    })
    hidden_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": hidden_url,
        "index": {},
        "body": {
            "name": "Hidden"
        },
    })
    trash_resource = Catalog(session, **{
        "element": "shoji:catalog",
        "self": trash_url,
        "index": {},
        "body": {
            "name": "Trash"
        },
    })
    session.add_fixture(folders_url, folders_resource)
    session.add_fixture(hidden_url, hidden_resource)
    session.add_fixture(trash_url, trash_resource)
    dataset = MutableDataset(dataset_resource)

    assert dataset.folders.root.name == "Root"
    assert dataset.folders.hidden.name == "Hidden"
    assert dataset.folders.trash.name == "Trash"
    assert not hasattr(dataset.folders, "secure")


def test_unique_folders_no_hidden():
    session = MockSession()
    dataset_url = 'http://host/api/datasets/abc/'
    folders_url = 'http://host/api/datasets/abc/folders/'
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
            "personal": "./personal/",
            "parents": "./parents/",
            # Viewer users don't have the secure folder available
            # "secure": secure_url,
            # Viewers also don't get the hidden folder exposed
            # "hidden": hidden_url,
            # Nor the trash
            # "trash": trash_url,
        }
    })
    session.add_fixture(folders_url, folders_resource)
    dataset = MutableDataset(dataset_resource)

    assert dataset.folders.root.name == "Root"
    assert not hasattr(dataset.folders, "secure")
    assert not hasattr(dataset.folders, "hidden")
    assert not hasattr(dataset.folders, "trash")
