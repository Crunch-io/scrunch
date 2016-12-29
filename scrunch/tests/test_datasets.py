import collections
import json

from unittest import mock
from unittest import TestCase

import pytest
from pandas import DataFrame
from pycrunch.elements import JSONObject
from pycrunch.shoji import Entity
from pycrunch.variables import cast

import scrunch
from scrunch.datasets import Dataset


class TestDatasetBase(object):

    def dataset_mock(self):
        variables = dict(
            disposition=dict(
                id='0001',
                alias='disposition',
                type='numeric',
                is_subvar=False
            ),
            exit_status=dict(
                id='0002',
                alias='exit_status',
                type='numeric',
                is_subvar=False
            ),
        )

        # Mocking setup.
        def _get(name):
            def f(*args):
                return variables[name].get(args[0], args[0])
            return f

        metadata = {}
        for alias in variables.keys():
            v = variables[alias]
            url = '%svariables/%s/' % (self.ds_url, v['id'])
            _m = mock.MagicMock()
            _m.entity.self = url
            _m.__getitem__.side_effect = _get(alias)
            _m.get.side_effect = _get(alias)
            metadata[alias] = _m

        class CrunchPayload(dict):
            def __getattr__(self, item):
                if item == 'payload':
                    return self
                else:
                    return self[item]

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return CrunchPayload({
                    'metadata': metadata
                })
            return CrunchPayload()

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.exclusion = '%sexclusion/' % self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.__class__ = Dataset
        ds.exclude = Dataset.exclude
        ds.session.get.side_effect = _session_get

        return Dataset(ds)


class TestExclusionFilters(TestDatasetBase, TestCase):
    ds_url = 'http://test.crunch.io/api/datasets/123/'

    def test_apply_exclusion(self):
        """
        Tests that the proper PATCH request is sent to Crunch in order to
        apply an exclusion filter to a dataset.
        """
        var_id = '0001'
        var_alias = 'disposition'
        var_type = 'numeric'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)

        # Mocking setup.
        def _get(*args):
            if args[0] == 'id':
                return var_id
            if args[0] == 'alias':
                return var_alias
            if args[0] == 'type':
                return var_type
            if args[0] == 'is_subvar':
                return False
            return args[0]

        _var_mock = mock.MagicMock()
        _var_mock.entity.self = var_url
        _var_mock.__getitem__.side_effect = _get
        _var_mock.get.side_effect = _get

        class CrunchPayload(dict):
            def __getattr__(self, item):
                if item == 'payload':
                    return self
                else:
                    return self[item]

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return CrunchPayload({
                    'metadata': {
                        var_alias: _var_mock
                    }
                })
            return CrunchPayload()

        ds_res = mock.MagicMock()
        ds_res.self = self.ds_url
        ds_res.fragments.exclusion = '%sexclusion/' % self.ds_url
        ds_res.fragments.table = '%stable/' % self.ds_url
        ds_res.session.get.side_effect = _session_get
        ds = Dataset(ds_res)

        # Action!
        exclusion_filter = 'disposition != 0'
        ds.exclude(exclusion_filter)

        # Ensure .patch was called the right way.
        assert len(ds.resource.session.patch.call_args_list) == 1

        call = ds.resource.session.patch.call_args_list[0]
        assert call[0][0] == ds.resource.fragments.exclusion

        expected_expr_obj = {
            'expression': {
                'function': '!=',
                'args': [
                    {'variable': var_url},  # Crunch needs variable URLs!
                    {'value': 0}
                ]
            }
        }
        assert json.loads(call[1]['data']) == expected_expr_obj

    def test_remove_exclusion(self):
        """
        Tests that the proper PATCH request is sent to Crunch in order to
        clear (i.e. remove) the exclusion filter from a dataset.
        """
        ds_res = mock.MagicMock()
        ds_res.fragments.exclusion = '%sexclusion/' % self.ds_url
        ds = Dataset(ds_res)
        ds.exclude()

        ds.resource.session.patch.assert_called_once_with(
            ds.resource.fragments.exclusion,
            data=json.dumps({'expression': {}})
        )

    def _exclude_payload(self, expr):
        dataset = self.dataset_mock()
        dataset.exclude(expr)
        call = dataset.session.patch.call_args_list[0]
        return json.loads(call[1]['data'])

    def test_gt(self):
        urld = '%svariables/%s/' % (self.ds_url, '0001')
        data = self._exclude_payload('disposition > 5')
        expected_expr_obj = {
            'expression': {
                'function': '>',
                'args': [
                    {'variable': urld},
                    {'value': 5}
                ]
            }
        }
        assert data == expected_expr_obj

    def test_in(self):
        data = self._exclude_payload('disposition in [32766]')
        expected_expr_obj = {
            "expression": {
                "function": "in",
                "args": [
                    {"variable": "http://test.crunch.io/api/datasets/123/variables/0001/"},
                    {"value": [32766]}
                ]
            }
        }

        assert data == expected_expr_obj

    def test_in_multiple(self):
        data = self._exclude_payload('disposition in (32766, 32767)')
        expected_expr_obj = {
            "expression": {
                "function": "in",
                "args": [
                    {"variable": "http://test.crunch.io/api/datasets/123/variables/0001/"},
                    {"value": [32766, 32767]}
                ]
            }
        }

        assert data == expected_expr_obj

    def test_not_and(self):
        data = self._exclude_payload('not (disposition in (1, 2) and exit_status == 0)')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "function": "and",
                        "args": [
                            {
                                "function": "in",
                                "args": [
                                    {
                                        "variable": "http://test.crunch.io/api/datasets/123/variables/0001/"
                                    },
                                    {
                                        "value": [
                                            1,
                                            2
                                        ]
                                    }
                                ]
                            },
                            {
                                "function": "==",
                                "args": [
                                    {
                                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                                    },
                                    {
                                        "value": 0
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_any(self):
        data = self._exclude_payload('exit_status.any([32766])')
        expected_expr_obj = {
            "expression": {
                "function": "any",
                "args": [
                    {
                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                    },
                    {
                        "value": [
                            32766
                        ]
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_not_any(self):
        data = self._exclude_payload('not exit_status.any([32766])')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "function": "any",
                        "args": [
                            {
                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                            },
                            {
                                "value": [
                                    32766
                                ]
                            }
                        ]
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_any_multiple(self):
        data = self._exclude_payload('exit_status.any([32766, 32767])')
        expected_expr_obj = {
            "expression": {
                "function": "any",
                "args": [
                    {
                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                    },
                    {
                        "value": [
                            32766,
                            32767
                        ]
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_all(self):
        data = self._exclude_payload('exit_status.all([32767])')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                    },
                    {
                        "value": [
                            32767
                        ]
                    }
                ],
                "function": "all"
            }
        }

        assert data == expected_expr_obj

    def test_not_all(self):
        data = self._exclude_payload('not exit_status.all([32767])')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "function": "all",
                        "args": [
                            {
                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                            },
                            {
                                "value": [
                                    32767
                                ]
                            }
                        ]
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_all_or_all(self):
        data = self._exclude_payload('exit_status.all([1]) or exit_status.all([2])')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "args": [
                            {
                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                            },
                            {
                                "value": [
                                    1
                                ]
                            }
                        ],
                        "function": "all"
                    },
                    {
                        "args": [
                            {
                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                            },
                            {
                                "value": [
                                    2
                                ]
                            }
                        ],
                        "function": "all"
                    }
                ],
                "function": "or"
            }
        }

        assert data == expected_expr_obj

    def test_not_all_or_all(self):
        data = self._exclude_payload('not(exit_status.all([1]) or exit_status.all([2]))')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "args": [
                            {
                                "args": [
                                    {
                                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                                    },
                                    {
                                        "value": [
                                            1
                                        ]
                                    }
                                ],
                                "function": "all"
                            },
                            {
                                "args": [
                                    {
                                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                                    },
                                    {
                                        "value": [
                                            2
                                        ]
                                    }
                                ],
                                "function": "all"
                            }
                        ],
                        "function": "or"
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_duplicates(self):
        data = self._exclude_payload('exit_status.duplicates()')
        expected_expr_obj = {
            "expression": {
                "function": "duplicates",
                "args": [
                    {
                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_valid(self):
        data = self._exclude_payload('valid(exit_status)')
        expected_expr_obj = {
            "expression": {
                "function": "is_valid",
                "args": [
                    {
                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_not_valid(self):
        data = self._exclude_payload('not valid(exit_status)')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "args": [
                            {
                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                            }
                        ],
                        "function": "is_valid"
                    }
                ],
                "function": "not"
            }
        }

        assert data == expected_expr_obj

    def test_missing(self):
        data = self._exclude_payload('missing(exit_status)')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                    }
                ],
                "function": "is_missing"
            }
        }

        assert data == expected_expr_obj

    def test_not_missing(self):
        data = self._exclude_payload('not missing(exit_status)')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "function": "is_missing",
                        "args": [
                            {
                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                            }
                        ]
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_equal(self):
        data = self._exclude_payload('exit_status == 1')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                    },
                    {
                        "value": 1
                    }
                ],
                "function": "=="
            }
        }

        assert data == expected_expr_obj

    def test_nested(self):
        data = self._exclude_payload('(disposition != 1 and (not valid(exit_status) or exit_status >= 1)) or (disposition == 0 and exit_status == 0) or (disposition == 0 and exit_status == 1)')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "args": [
                            {
                                "args": [
                                    {
                                        "variable": "http://test.crunch.io/api/datasets/123/variables/0001/"
                                    },
                                    {
                                        "value": 1
                                    }
                                ],
                                "function": "!="
                            },
                            {
                                "args": [
                                    {
                                        "args": [
                                            {
                                                "args": [
                                                    {
                                                        "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                                                    }
                                                ],
                                                "function": "is_valid"
                                            }
                                        ],
                                        "function": "not"
                                    },
                                    {
                                        "args": [
                                            {
                                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                                            },
                                            {
                                                "value": 1
                                            }
                                        ],
                                        "function": ">="
                                    }
                                ],
                                "function": "or"
                            }
                        ],
                        "function": "and"
                    },
                    {
                        "args": [
                            {
                                "args": [
                                    {
                                        "args": [
                                            {
                                                "variable": "http://test.crunch.io/api/datasets/123/variables/0001/"
                                            },
                                            {
                                                "value": 0
                                            }
                                        ],
                                        "function": "=="
                                    },
                                    {
                                        "args": [
                                            {
                                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                                            },
                                            {
                                                "value": 0
                                            }
                                        ],
                                        "function": "=="
                                    }
                                ],
                                "function": "and"
                            },
                            {
                                "args": [
                                    {
                                        "args": [
                                            {
                                                "variable": "http://test.crunch.io/api/datasets/123/variables/0001/"
                                            },
                                            {
                                                "value": 0
                                            }
                                        ],
                                        "function": "=="
                                    },
                                    {
                                        "args": [
                                            {
                                                "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                                            },
                                            {
                                                "value": 1
                                            }
                                        ],
                                        "function": "=="
                                    }
                                ],
                                "function": "and"
                            }
                        ],
                        "function": "or"
                    }
                ],
                "function": "or"
            }
        }

        assert data == expected_expr_obj

    def test_dict_expr(self):
        expr = {
            "args": [
                {
                    "variable": "http://test.crunch.io/api/datasets/123/variables/0002/"
                },
                {
                    "value": 1
                }
            ],
            "function": "=="
        }
        data = self._exclude_payload(expr)
        expected_expr_obj = {'expression': expr}
        assert data == expected_expr_obj


class TestVariables(TestCase):
    def test_variable_as_attribute(self):
        session = mock.MagicMock()
        dataset_resource = mock.MagicMock()
        dataset_resource.session = session

        test_variable = mock.MagicMock()
        test_variable.entity = Entity(session=session)

        variables = {
            'test_variable': test_variable
        }
        dataset_resource.variables = mock.MagicMock()
        dataset_resource.variables.by.return_value = variables

        dataset = Dataset(dataset_resource)

        assert isinstance(dataset.test_variable, Entity)
        with pytest.raises(AttributeError) as err:
            dataset.another_variable

        assert str(err.value) == 'Dataset has no attribute another_variable'

    def test_variable_cast(self):
        variable = mock.MagicMock()
        cast(
            variable,
            type='numeric',
            offset='offset',
            resolution='resolution',
            format='format'
        )
        call = variable.cast.post.call_args_list[0]
        data = json.loads(call[1]['data'])
        assert data == {
            'cast_as': 'numeric',
            'resolution': 'resolution',
            'offset': 'offset',
            'format': 'format'
        }


class TestCurrentEditor(TestDatasetBase, TestCase):
    ds_url = 'https://test.crunch.io/api/datasets/123456/'
    user_url = 'https://test.crunch.io/api/users/12345/'

    def test_change_editor(self):
        body = {
            'self': self.ds_url,
            'name': 'Dataset Name'
        }
        sess = mock.MagicMock()
        ds_res = mock.MagicMock(session=sess, body=body)
        ds_res.patch = mock.MagicMock()
        ds = Dataset(ds_res)
        ds.change_editor(self.user_url)

        ds_res.patch.assert_called_with({
            'current_editor': self.user_url
        })

    def test_change_editor_email(self):
        sess = mock.MagicMock()
        response = mock.MagicMock()
        response.payload = {
            'index': {
                self.user_url: {
                    'email': 'jane.doe@crunch.io'
                }
            }
        }

        def _get(*args, **kwargs):
            return response

        sess.get.side_effect = _get
        ds_res = mock.MagicMock(session=sess)
        ds_res.self = self.ds_url
        ds_res.patch = mock.MagicMock()
        ds = Dataset(ds_res)
        ds.change_editor('jane.doe@crunch.io')

        ds_res.patch.assert_called_with({
            'current_editor': self.user_url
        })


class TestSavepoints(TestCase):

    ds_url = 'http://test.crunch.io/api/datasets/123/'

    def test_create_savepoint(self):
        sess = mock.MagicMock()
        ds_res = mock.MagicMock(session=sess)
        ds_res.savepoints = mock.MagicMock()
        ds = Dataset(ds_res)
        ds.create_savepoint('savepoint description')
        ds_res.savepoints.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'description': 'savepoint description'
            }
        })

    def test_create_savepoint_keyerror(self):
        sess = mock.MagicMock()
        ds_res = mock.MagicMock(session=sess)
        ds_res.savepoints = mock.MagicMock()
        ds_res.savepoints.index = {
            1: {
                'description': 'savepoint description'
            }
        }
        ds = Dataset(ds_res)
        with pytest.raises(KeyError):
            ds.create_savepoint('savepoint description')

    def test_load_initial_savepoint(self):
        sess = mock.MagicMock()
        ds_res = mock.MagicMock(session=sess)
        ds_res.savepoints = mock.MagicMock()
        ds_res.savepoints.index = {
            1: {
                'description': 'savepoint description'
            }
        }
        ds = Dataset(ds_res)
        with pytest.raises(KeyError):
            ds.create_savepoint('savepoint description')

    def test_load_empty_savepoint(self):
        sess = mock.MagicMock()
        ds_res = mock.MagicMock(session=sess)
        ds_res.savepoints = mock.MagicMock()
        ds_res.savepoints.index = {}
        ds = Dataset(ds_res)
        with pytest.raises(KeyError):
            ds.load_savepoint('savepoint')


class TestForks(TestCase):

    ds_url = 'http://test.crunch.io/api/datasets/123/'

    def test_fork(self):
        sess = mock.MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'description': 'ds description',
            'owner': 'http://test.crunch.io/api/users/123/'
        })
        ds_res = mock.MagicMock(session=sess, body=body)
        ds_res.forks = mock.MagicMock()
        ds_res.forks.index = {}
        ds = Dataset(ds_res)
        f = ds.fork()
        ds_res.forks.create.assert_called_with({
            'body': {
                'name': 'FORK #1 of ds name',
                'description': 'ds description',
                'is_published': False,
            }
        })
        f.resource.savepoints.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'description': 'initial fork'
            }
        })

    def test_fork_preserve_owner(self):
        user_id = 'http://test.crunch.io/api/users/123/'
        sess = mock.MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'description': 'ds description',
            'owner': user_id
        })
        ds_res = mock.MagicMock(session=sess, body=body)
        ds_res.forks = mock.MagicMock()
        ds_res.forks.index = {}
        ds = Dataset(ds_res)
        f = ds.fork(preserve_owner=True)
        f.resource.patch.assert_called_with({'owner': user_id})

    def test_fork_preserve_owner_project(self):
        project_id = 'http://test.crunch.io/api/projects/456/'
        sess = mock.MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'description': 'ds description',
            'owner': project_id
        })
        ds_res = mock.MagicMock(session=sess, body=body)
        ds_res.forks = mock.MagicMock()
        ds_res.forks.index = {}
        ds = Dataset(ds_res)
        f = ds.fork()
        f.resource.patch.assert_called_with({'owner': project_id})

    def test_delete_forks(self):
        f1 = mock.MagicMock()
        f2 = mock.MagicMock()
        f3 = mock.MagicMock()
        sess = mock.MagicMock()
        ds_res = mock.MagicMock(session=sess)
        ds_res.forks = mock.MagicMock()
        ds_res.forks.index = {
            'abc1': f1,
            'abc2': f2,
            'abc3': f3
        }

        ds = Dataset(ds_res)
        ds.delete_forks()

        assert f1.entity.delete.call_count == 1
        assert f2.entity.delete.call_count == 1
        assert f3.entity.delete.call_count == 1

    def test_forks_dataframe(self):
        f1 = dict(
            name='name',
            description='description',
            is_published=True,
            owner_name='Jane Doe',
            current_editor_name='John Doe',
            creation_time='2016-01-01T00:00Z',
            modification_time='2016-01-01T00:00Z',
            id='abc123',
        )
        sess = mock.MagicMock()
        ds_res = mock.Mock(session=sess)
        ds_res.forks = mock.MagicMock()
        ds_res.forks.index = {
            'abc1': f1
        }

        ds = Dataset(ds_res)
        df = ds.forks_dataframe()
        assert isinstance(df, DataFrame)
        keys = [k for k in df.keys()]
        assert keys == [
            'name', 'description', 'is_published', 'owner_name',
            'current_editor_name', 'creation_time', 'modification_time', 'id'
        ]

    def test_forks_dataframe_empty(self):
        sess = mock.MagicMock()
        ds_res = mock.Mock(session=sess)
        ds_res.forks = mock.MagicMock()
        ds_res.forks.index = {}

        ds = Dataset(ds_res)
        df = ds.forks_dataframe()

        assert df is None


class TestHierarchicalOrder(TestCase):

    ds_url = 'http://test.crunch.local/api/datasets/123/'

    class CrunchPayload(dict):
        def __init__(self, *args, **kwargs):
            super(self.__class__, self).__init__(*args, **kwargs)
            self.put = mock.MagicMock()

        def __getattr__(self, item):
            if item == 'payload':
                return self
            else:
                return self[item]

    @staticmethod
    def _build_get_func(var):
        properties = {}
        properties.update(var)

        def _get(*args):
            return properties.get(args[0], args[0])

        return _get

    @staticmethod
    def _get_update_payload(ds):
        try:
            return ds.order.hier.put.call_args_list[-1][0][0]
        except IndexError:
            return None

    def setUp(self):
        variable_defs = [
            {
                'id': '000001',
                'alias': 'id',
                'name': 'ID',
                'type': 'numeric',
                'is_subvar': False
            },
            {
                'id': '000002',
                'alias': 'hobbies',
                'name': 'Hobbies',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000003',
                'alias': 'registration_time',
                'name': 'Registration Time',
                'type': 'numeric',
                'is_subvar': False
            },
            {
                'id': '000004',
                'alias': 'last_login_time',
                'name': 'Last Login Time',
                'type': 'numeric',
                'is_subvar': False
            },
            {
                'id': '000005',
                'alias': 'first_name',
                'name': 'First Name',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000006',
                'alias': 'last_name',
                'name': 'Last Name',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000007',
                'alias': 'gender',
                'name': 'Gender',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000008',
                'alias': 'country',
                'name': 'Country',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000009',
                'alias': 'city',
                'name': 'City',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000010',
                'alias': 'zip_code',
                'name': 'Zip Code',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000011',
                'alias': 'address',
                'name': 'Address',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000012',
                'alias': 'music',
                'name': 'Music',
                'type': 'text',
                'is_subvar': False
            },
            {
                'id': '000013',
                'alias': 'religion',
                'name': 'Religion',
                'type': 'text',
                'is_subvar': False
            }
        ]
        table = {
            'element': 'crunch:table',
            'self': '%stable/' % self.ds_url,
            'metadata': collections.OrderedDict()
        }
        variables = collections.OrderedDict()
        hier_order = {
            'element': 'shoji:order',
            'self': '%svariables/hier/' % self.ds_url,
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/'                        # religion
            ]
        }

        for var in variable_defs:
            var_url = '%svariables/%s/' % (self.ds_url, var['id'])
            _get_func = self._build_get_func(var)
            _var_mock = mock.MagicMock()
            _var_mock.__getitem__.side_effect = _get_func
            _var_mock.get.side_effect = _get_func
            _var_mock.entity.self = var_url
            _var_mock.entity.body.__getitem__.side_effect = _get_func
            _var_mock.entity.body.get.side_effect = _get_func
            table['metadata'][var['id']] = _var_mock
            variables[var['id']] = _var_mock

        def _session_get(*args):
            if args[0] == '{}table/'.format(self.ds_url):
                return self.CrunchPayload(table)
            elif args[0] == '{}variables/hier/'.format(self.ds_url):
                return self.CrunchPayload(hier_order)
            return self.CrunchPayload()

        ds_resource = mock.MagicMock()
        ds_resource.self = self.ds_url
        ds_resource.variables.orders.hier = '%svariables/hier/' % self.ds_url
        ds_resource.variables.by.return_value = variables
        ds_resource.session.get.side_effect = _session_get
        self.ds = Dataset(ds_resource)

    def test_order_property_is_loaded_correctly(self):
        ds = self.ds

        assert isinstance(ds.order, scrunch.datasets.Order)
        assert isinstance(ds.order.graph, scrunch.datasets.Group)  # root group

    def test_element_access(self):
        ds = self.ds

        # Test element access using 0-based integer indexes.
        var = ds.order[0]
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'ID'
        assert var.alias == 'id'
        assert var.id == '000001'

        group = ds.order[2]
        assert isinstance(group, scrunch.datasets.Group)
        assert group.name == 'Account'
        assert group.parent == ds.order.graph

        # Test element access using "element references".
        var = ds.order['id']
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'ID'
        assert var.alias == 'id'
        assert var.id == '000001'

        group = ds.order['Account']
        assert isinstance(group, scrunch.datasets.Group)
        assert group.name == 'Account'
        assert group.parent == ds.order.graph

        # Test that the `in` operator works.
        assert all(
            isinstance(obj, (scrunch.datasets.Variable, scrunch.datasets.Group))
            for obj in ds.order
        )
        assert 'id' in ds.order
        assert 'Account' in ds.order
        assert 'invalid_alias' not in ds.order

    def test_nested_element_access(self):
        ds = self.ds

        # Test nested element access using 0-based integer indexes.
        var = ds.order[2][0]
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Registration Time'
        assert var.alias == 'registration_time'
        assert var.id == '000003'

        var = ds.order[2][3][0]
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Country'
        assert var.alias == 'country'
        assert var.id == '000008'

        group = ds.order[2][2]
        assert isinstance(group, scrunch.datasets.Group)
        assert group.name == 'User Information'
        assert group.parent == ds.order[2]

        # Test nested element access using "element references".
        var = ds.order['Account']['registration_time']
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Registration Time'
        assert var.alias == 'registration_time'
        assert var.id == '000003'

        var = ds.order['Account']['Location']['country']
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Country'
        assert var.alias == 'country'
        assert var.id == '000008'

        group = ds.order['Account']['User Information']
        assert isinstance(group, scrunch.datasets.Group)
        assert group.name == 'User Information'
        assert group.parent == ds.order['Account']

        # Test mixed nested element access
        var = ds.order['Account'][0]
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Registration Time'
        assert var.alias == 'registration_time'
        assert var.id == '000003'

        var = ds.order[2]['Location'][0]
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Country'
        assert var.alias == 'country'
        assert var.id == '000008'

        group = ds.order[2]['User Information']
        assert isinstance(group, scrunch.datasets.Group)
        assert group.name == 'User Information'
        assert group.parent == ds.order['Account']

        # Test that the `in` operator works with nested access.
        assert all(
            isinstance(obj, (scrunch.datasets.Variable, scrunch.datasets.Group))
            for obj in ds.order['Account']
        )
        assert 'registration_time' in ds.order['Account']
        assert 'User Information' in ds.order[2]
        assert 'country' in ds.order[2]['Location']
        assert 'invalid_alias' not in ds.order['Account']
        assert 'invalid_alias' not in ds.order['Account']['Location']

        # Test element access with the `hierarchy` and `variables` properties.
        var = ds.order['Account'].hierarchy[0]
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Registration Time'
        assert var.alias == 'registration_time'
        assert var.id == '000003'

        var = ds.order.variables['country']
        assert isinstance(var, scrunch.datasets.Variable)
        assert var.name == 'Country'
        assert var.alias == 'country'
        assert var.id == '000008'

    def test_element_str_representation(self):
        ds = self.ds

        # Test first-level str representation.
        assert str(ds.order) == json.dumps(
            ['id', 'hobbies', 'Group(Account)', 'music', 'religion'],
            indent=scrunch.datasets.AbstractContainer.indent_size
        )

        # Test nested str representation.
        assert str(ds.order['Account']) == json.dumps(
            [
                'registration_time',
                'last_login_time',
                'Group(User Information)',
                'Group(Location)'
            ],
            indent=scrunch.datasets.AbstractContainer.indent_size
        )

        # Test the str representation of the `hierarchy`.
        assert str(ds.order.hierarchy) == json.dumps(
            [
                'id',
                'hobbies',
                {
                    'Account': [
                        'registration_time',
                        'last_login_time',
                        {
                            'User Information': [
                                'first_name',
                                'last_name',
                                'gender'
                            ]
                        },
                        {
                            'Location': [
                                'country',
                                'city',
                                'zip_code',
                                'address'
                            ]
                        }
                    ]
                },
                'music',
                'religion'
            ],
            indent=scrunch.datasets.AbstractContainer.indent_size
        )

        # Test the str representation of the `hierarchy` in nested Groups.
        assert str(ds.order['Account'].hierarchy) == json.dumps(
            [
                'registration_time',
                'last_login_time',
                {
                    'User Information': [
                        'first_name',
                        'last_name',
                        'gender'
                    ]
                },
                {
                    'Location': [
                        'country',
                        'city',
                        'zip_code',
                        'address'
                    ]
                }
            ],
            indent=scrunch.datasets.AbstractContainer.indent_size
        )

        # Test the str representation of the flat list of `variables`.
        assert str(ds.order.variables) == json.dumps(
            [
                'id',
                'hobbies',
                'registration_time',
                'last_login_time',
                'first_name',
                'last_name',
                'gender',
                'country',
                'city',
                'zip_code',
                'address',
                'music',
                'religion'
            ],
            indent=scrunch.datasets.AbstractContainer.indent_size
        )

        # Test the str representation of the flat list of `variables`
        # in nested groups.
        assert str(ds.order['Account'].variables) == json.dumps(
            [
                'registration_time',
                'last_login_time',
                'first_name',
                'last_name',
                'gender',
                'country',
                'city',
                'zip_code',
                'address'
            ],
            indent=scrunch.datasets.AbstractContainer.indent_size
        )

    def test_update_hierarchy_order(self):
        ds = self.ds
        ds.order.update()
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

    def test_local_movements(self):
        ds = self.ds

        ds.order.move('id')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000001/',                       # id
            ]
        }

        ds.order.move('music', position=1)
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000002/',                       # hobbies
                '../000012/',                       # music
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000013/',                       # religion
                '../000001/',                       # id
            ]
        }

        ds.order.move(elements=['id', 'Account'], position=3)
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000002/',                       # hobbies
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000001/',                       # id
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
            ]
        }

        ds.order.move_top(['Account', 'id'])
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000001/',                       # id
                '../000002/',                       # hobbies
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order.move_bottom('hobbies')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000001/',                       # id
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000002/',                       # hobbies
            ]
        }

        ds.order.move_before('music', ['hobbies', 'religion'])
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000001/',                       # id
                '../000002/',                       # hobbies
                '../000013/',                       # religion
                '../000012/',                       # music
            ]
        }

        ds.order.move_after('id', 'Account')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000002/',                       # hobbies
                '../000013/',                       # religion
                '../000012/',                       # music
            ]
        }

        ds.order.move_up('religion')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000013/',                       # religion
                '../000002/',                       # hobbies
                '../000012/',                       # music
            ]
        }

        ds.order.move_down(element='Account')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000013/',                       # religion
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000002/',                       # hobbies
                '../000012/',                       # music
            ]
        }

    def test_local_movements_with_nested_access(self):
        ds = self.ds

        ds.order['Account'].move('registration_time')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        },
                        '../000003/',               # registration_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account']['User Information'].move('gender', position=1)
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000007/',       # gender
                                '../000006/',       # last_name
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        },
                        '../000003/',               # registration_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account']['Location'].move(['country', 'zip_code'], 2)
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000007/',       # gender
                                '../000006/',       # last_name
                            ]
                        },
                        {
                            'Location': [
                                '../000009/',       # city
                                '../000011/',       # address
                                '../000008/',       # country
                                '../000010/',       # zip_code
                            ]
                        },
                        '../000003/',               # registration_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account']['Location'].move_top('address')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000007/',       # gender
                                '../000006/',       # last_name
                            ]
                        },
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000009/',       # city
                                '../000008/',       # country
                                '../000010/',       # zip_code
                            ]
                        },
                        '../000003/',               # registration_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account'].move_bottom('User Information')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000004/',               # last_login_time
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000009/',       # city
                                '../000008/',       # country
                                '../000010/',       # zip_code
                            ]
                        },
                        '../000003/',               # registration_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000007/',       # gender
                                '../000006/',       # last_name
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account'].move_before('last_login_time', 'Location')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000009/',       # city
                                '../000008/',       # country
                                '../000010/',       # zip_code
                            ]
                        },
                        '../000004/',               # last_login_time
                        '../000003/',               # registration_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000007/',       # gender
                                '../000006/',       # last_name
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account']['Location'].move_after('country', 'city')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                            ]
                        },
                        '../000004/',               # last_login_time
                        '../000003/',               # registration_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000007/',       # gender
                                '../000006/',       # last_name
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account']['User Information'].move_up('gender')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                            ]
                        },
                        '../000004/',               # last_login_time
                        '../000003/',               # registration_time
                        {
                            'User Information': [
                                '../000007/',       # gender
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        ds.order['Account'].move_down(element='registration_time')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                            ]
                        },
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000007/',       # gender
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                            ]
                        },
                        '../000003/',               # registration_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

    def test_cross_group_movements(self):
        ds = self.ds

        ds.order.move('gender')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/',       # address
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000007/',                       # gender
            ]
        }

        ds.order.move('address', position=1)
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000011/',                       # address
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000007/',                       # gender
            ]
        }

        ds.order.move(elements=['last_login_time', 'Location'], position=3)
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000011/',                       # address
                '../000002/',                       # hobbies
                '../000004/',                       # last_login_time
                {
                    'Location': [
                        '../000008/',               # country
                        '../000009/',               # city
                        '../000010/',               # zip_code
                    ]
                },
                {
                    'Account': [
                        '../000003/',               # registration_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000007/',                       # gender
            ]
        }

        ds.order.move_top(['User Information', 'country'])
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                {
                    'User Information': [
                        '../000005/',              # first_name
                        '../000006/',              # last_name
                    ]
                },
                '../000008/',                       # country
                '../000001/',                       # id
                '../000011/',                       # address
                '../000002/',                       # hobbies
                '../000004/',                       # last_login_time
                {
                    'Location': [
                        '../000009/',               # city
                        '../000010/',               # zip_code
                    ]
                },
                {
                    'Account': [
                        '../000003/',               # registration_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000007/',                       # gender
            ]
        }

        ds.order.move_bottom('zip_code')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                {
                    'User Information': [
                        '../000005/',              # first_name
                        '../000006/',              # last_name
                    ]
                },
                '../000008/',                       # country
                '../000001/',                       # id
                '../000011/',                       # address
                '../000002/',                       # hobbies
                '../000004/',                       # last_login_time
                {
                    'Location': [
                        '../000009/',               # city
                    ]
                },
                {
                    'Account': [
                        '../000003/',               # registration_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000007/',                       # gender
                '../000010/',                       # zip_code
            ]
        }

#----

        ds.order['Account'].move('last_login_time')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                {
                    'User Information': [
                        '../000005/',              # first_name
                        '../000006/',              # last_name
                    ]
                },
                '../000008/',                       # country
                '../000001/',                       # id
                '../000011/',                       # address
                '../000002/',                       # hobbies
                {
                    'Location': [
                        '../000009/',               # city
                    ]
                },
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000007/',                       # gender
                '../000010/',                       # zip_code
            ]
        }

        ds.order['User Information'].move_bottom('gender')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                {
                    'User Information': [
                        '../000005/',              # first_name
                        '../000006/',              # last_name
                        '../000007/',              # gender
                    ]
                },
                '../000008/',                       # country
                '../000001/',                       # id
                '../000011/',                       # address
                '../000002/',                       # hobbies
                {
                    'Location': [
                        '../000009/',               # city
                    ]
                },
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000010/',                       # zip_code
            ]
        }

        ds.order['Account'].move(['User Information', 'Location'])
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000008/',                       # country
                '../000001/',                       # id
                '../000011/',                       # address
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/',       # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000009/',       # city
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000010/',                       # zip_code
            ]
        }

        ds.order['Account']['Location'].move_top('address')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000008/',                       # country
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/',       # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000009/',       # city
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000010/',                       # zip_code
            ]
        }

        ds.order['Account']['Location'].move_after('city', 'country')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/',       # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000011/',       # address
                                '../000009/',       # city,
                                '../000008/',       # country
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000010/',                       # zip_code
            ]
        }

    def test_group_level_reordering(self):
        ds = self.ds

        ds.order.set(['id', 'hobbies', 'music', 'religion', 'Account'])
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                '../000012/',                       # music
                '../000013/',                       # religion
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
            ]
        }

        ds.order['Account'].set(
            ['User Information', 'Location', 'registration_time',
             'last_login_time']
        )
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                '../000012/',                       # music
                '../000013/',                       # religion
                {
                    'Account': [
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        },
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                    ]
                },
            ]
        }

    def test_element_removal(self):
        ds = self.ds

        ds.order['Account'].remove('last_login_time')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000004/',                       # last_login_time
            ]
        }

        ds.order['Account']['Location'].remove(['country', 'city'])
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000004/',                       # last_login_time
                '../000008/',                       # country
                '../000009/',                       # city
            ]
        }

        with pytest.raises(NotImplementedError):
            ds.order.remove('id')

    def test_group_deletion(self):
        ds = self.ds

        ds.order['Account'].delete()
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                '../000012/',                       # music
                '../000013/',                       # religion
                '../000003/',                       # registration_time
                '../000004/',                       # last_login_time
                {
                    'User Information': [
                        '../000005/',               # first_name
                        '../000006/',               # last_name
                        '../000007/',               # gender
                    ]
                },
                {
                    'Location': [
                        '../000008/',               # country
                        '../000009/',               # city
                        '../000010/',               # zip_code
                        '../000011/',               # address
                    ]
                }
            ]
        }

        with pytest.raises(NotImplementedError):
            ds.order.delete()

    def test_group_creation(self):
        ds = self.ds

        ds.order['Account'].create(
            'Login Details', elements=['registration_time', 'last_login_time']
        )
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        {
                            'User Information': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/',       # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/',       # address
                            ]
                        },
                        {
                            'Login Details': [
                                '../000003/',       # registration_time
                                '../000004/',       # last_login_time
                            ]
                        },
                    ]
                },
                '../000012/',                       # music
                '../000013/',                       # religion
            ]
        }

        with pytest.raises(ValueError):
            ds.order.create('Account')

    def test_group_renaming(self):
        ds = self.ds

        ds.order['Account']['User Information'].rename('User Info')
        assert self._get_update_payload(ds) == {
            'element': 'shoji:order',
            'graph': [
                '../000001/',                       # id
                '../000002/',                       # hobbies
                {
                    'Account': [
                        '../000003/',               # registration_time
                        '../000004/',               # last_login_time
                        {
                            'User Info': [
                                '../000005/',       # first_name
                                '../000006/',       # last_name
                                '../000007/'        # gender
                            ]
                        },
                        {
                            'Location': [
                                '../000008/',       # country
                                '../000009/',       # city
                                '../000010/',       # zip_code
                                '../000011/'        # address
                            ]
                        }
                    ]
                },
                '../000012/',                       # music
                '../000013/'                        # religion
            ]
        }

        with pytest.raises(NotImplementedError):
            ds.order.rename('Root')

        with pytest.raises(ValueError):
            ds.order['Account'].rename('id')
