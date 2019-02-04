from mock import MagicMock
from scrunch.folders import Folder

from .test_datasets import AttributeDict


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
