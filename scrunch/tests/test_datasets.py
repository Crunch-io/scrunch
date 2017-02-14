import collections
import json

import mock
from unittest import TestCase

import pytest
from pandas import DataFrame
from pycrunch.elements import JSONObject
from pycrunch.variables import cast

import scrunch
from scrunch.datasets import Dataset, Variable


class _CrunchPayload(dict):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.put = mock.MagicMock()
        self.post = mock.MagicMock()
        self.patch = mock.MagicMock()

    def __getattr__(self, item):
        if item == 'payload':
            return self
        else:
            return self[item]


def _build_get_func(var):
    properties = {}
    properties.update(var)

    def _get(*args):
        return properties.get(args[0], args[0])

    return _get


class TestDatasetBase(object):
    ds_url = 'https://test.crunch.io/api/datasets/123456/'

    def dataset_mock(self, variables=None):
        variables = variables or {
            '0001': dict(
                id='0001',
                alias='disposition',
                type='numeric',
                is_subvar=False
            ),
            '0002': dict(
                id='0002',
                alias='exit_status',
                type='numeric',
                is_subvar=False
            )
        }

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.exclusion = '%sexclusion/' % self.ds_url
        table_mock = mock.MagicMock(metadata=variables)
        ds.follow.return_value = table_mock

        return Dataset(ds)


class TestDatasetBaseNG(object):
    ds_url = 'https://test.crunch.io/api/datasets/123456/'

    ds_shoji = {
        'element': 'shoji:entity',
        'body': {
            'id': '123456',
            'name': 'test_dataset_name',
            'alias': 'test_dataset_alias'
        }
    }

    variables = {
        '0001': dict(
            id='0001',
            alias='var1_alias',
            name='var1_name',
            type='numeric',
            is_subvar=False
        ),
        '0002': dict(
            id='0002',
            alias='var2_alias',
            name='var2_name',
            type='numeric',
            is_subvar=False
        )
    }

    def _dataset_mock(self, variables=None):
        _ds_mock = mock.MagicMock()
        _get_body = self._build_get_func(self.ds_shoji['body'])
        _ds_mock.variables.by.side_effect = self._variables_by_side_effect(variables)
        _ds_mock.entity.self = self.ds_url
        _ds_mock.entity.body.__getitem__.side_effect = _get_body
        _ds_mock.entity.body.get.side_effect = _get_body
        _ds_mock.body.__getitem__.side_effect = _get_body
        _ds_mock.body.get.side_effect = _get_body
        return _ds_mock

    def _variable_mock(self, variable=None):
        variable = variable or self.variables['0001']

        var_url = '%svariables/%s/' % (self.ds_url, variable['id'])
        _get_func = self._build_get_func(variable)
        _var_mock = mock.MagicMock()
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func
        _var_mock.entity.self = var_url
        _var_mock.entity.body.__getitem__.side_effect = _get_func
        _var_mock.entity.body.get.side_effect = _get_func
        return _var_mock

    def _variables_by_side_effect(self, variables=None):
        variables = variables or self.variables
        table = {
            'element': 'crunch:table',
            'self': '%stable/' % self.ds_url,
            'metadata': collections.OrderedDict()
        }

        _variables = dict(id=dict(), name=dict(), alias=dict())
        for var in variables:
            _var_mock = self._variable_mock(variables[var])
            _variables['id'].update({variables[var]['id']: _var_mock})
            _variables['name'].update({variables[var]['name']: _var_mock})
            _variables['alias'].update({variables[var]['alias']: _var_mock})
            table['metadata'][variables[var]['id']] = _var_mock

        self.ds_shoji['body']['table'] = table

        def _get(*args):
            return _variables.get(args[0])
        return _get

    @staticmethod
    def _build_get_func(d):
        properties = {}
        properties.update(d)

        def _get(*args):
            return properties.get(args[0])
        return _get

    @staticmethod
    def _by_side_effect(shoji, entity_mock):
        d = {'name': {shoji['body']['name']: entity_mock},
             'id': {shoji['body']['id']: entity_mock},
             'alias': {shoji['body']['alias']: entity_mock}}

        def _get(*args):
            return d.get(args[0])
        return _get


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

        ds_res = mock.MagicMock()
        ds_res.self = self.ds_url
        table_mock = mock.MagicMock(metadata={
            var_id: {
                'alias': var_alias,
                'id': var_id,
                'type': var_type,
            }
        })
        ds_res.follow.return_value = table_mock
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


class TestVariables(TestDatasetBaseNG, TestCase):
    def test_variable_as_member(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        assert ds.name == self.ds_shoji['body']['name']
        assert ds.id == self.ds_shoji['body']['id']

        assert isinstance(ds['var1_alias'], Variable)

        with pytest.raises(ValueError) as err:
            ds['some_variable']
        assert str(err.value) == \
            'Dataset %s has no variable some_variable' % ds.name

        with pytest.raises(AttributeError) as err:
            ds.some_variable
        assert str(err.value) == \
            'Dataset %s has no attribute some_variable' % ds.name

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


class TestRecode(TestDatasetBase):
    def test_recode_single_categorical(self):
        variables = {
            '001': {
                'id': '001',
                'alias': 'var_a',
                'name': 'Variable A',
                'type': 'numeric',
                'is_subvar': False
            },
            '002': {
                'id': '002',
                'alias': 'var_b',
                'name': 'Variable B',
                'type': 'categorical',
                'is_subvar': False
            },
            '003': {
                'id': '003',
                'alias': 'var_c',
                'name': 'Variable C',
                'type': 'categorical',
                'is_subvar': False
            },
            '004': {
                'id': '004',
                'alias': 'gender',
                'name': 'Gender',
                'type': 'categorical',
                'is_subvar': False
            },
            '005': {
                'id': '005',
                'alias': 'age',
                'name': 'Age',
                'type': 'categorical',
                'is_subvar': False
            },
        }
        ds = self.dataset_mock(variables=variables)
        responses = [
            {'id': 1, 'name': 'Facebook', 'case': 'var_a > 5'},
            {'id': 2, 'name': 'Twitter',
             'case': 'var_b < 10 and var_c in (1, 2, 3)'},
            {'id': 3, 'name': 'Google+',
             'case': '(gender == 1) and (age >= 16 and age <= 24)'},
        ]
        ds.create_categorical(responses, alias='cat', name='My cat', multiple=False)
        ds.resource.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'description': '',
                'alias': 'cat',
                'name': 'My cat',
                'expr': {
                    'function': 'case',
                    'args': [{
                        'column': [1, 2, 3, -1],
                        'type': {
                            'value': {
                                'class': 'categorical',
                                'categories': [
                                    {'missing': False, 'id': 1, 'name': 'Facebook'},
                                    {'missing': False, 'id': 2, 'name': 'Twitter'},
                                    {'missing': False, 'id': 3, 'name': 'Google+'},
                                    {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
                                ]
                            }
                        }
                    }, {
                        'function': '>',
                        'args': [
                            {'variable': 'https://test.crunch.io/api/datasets/123456/variables/001/'},
                            {'value': 5}
                        ]
                    }, {
                        'function': 'and',
                        'args': [{
                            'function': '<',
                            'args': [
                                {'variable': 'https://test.crunch.io/api/datasets/123456/variables/002/'},
                                {'value': 10}
                            ]}, {
                            'function': 'in',
                            'args': [
                                {'variable': 'https://test.crunch.io/api/datasets/123456/variables/003/'},
                                {'value': [1, 2, 3]}
                            ]
                        }]
                    }, {
                        'function': 'and',
                        'args': [{
                            'function': '==',
                            'args': [
                                {'variable': 'https://test.crunch.io/api/datasets/123456/variables/004/'},
                                {'value': 1}
                            ]
                        }, {
                            'function': 'and',
                            'args': [{
                                'function': '>=',
                                'args': [
                                    {'variable': 'https://test.crunch.io/api/datasets/123456/variables/005/'},
                                    {'value': 16}
                                ]
                            }, {
                                'function': '<=',
                                'args': [
                                    {'variable': 'https://test.crunch.io/api/datasets/123456/variables/005/'},
                                    {'value': 24}
                                ]
                            }]
                        }]
                    }]
                }
            },
        })

    def test_recode_multiple_response(self):
        variables = {
            'var_a': {
                'id': '001',
                'alias': 'var_a',
                'name': 'Variable A',
                'type': 'numeric',
                'is_subvar': False
            },
            'var_b': {
                'id': '002',
                'alias': 'var_b',
                'name': 'Variable B',
                'type': 'categorical',
                'is_subvar': False
            },
            'var_c': {
                'id': '003',
                'alias': 'var_c',
                'name': 'Variable C',
                'type': 'categorical',
                'is_subvar': False
            },
            'gender': {
                'id': '004',
                'alias': 'gender',
                'name': 'Gender',
                'type': 'categorical',
                'is_subvar': False
            },
            'age': {
                'id': '005',
                'alias': 'age',
                'name': 'Age',
                'type': 'categorical',
                'is_subvar': False
            },
        }
        ds = self.dataset_mock(variables=variables)
        responses = [
            {'id': 1, 'name': 'Facebook', 'case': 'var_a > 5'},
            {'id': 2, 'name': 'Twitter', 'case': 'var_b < 10 and var_c in (1, 2, 3)'},
            {'id': 3, 'name': 'Google+', 'case': '(gender == 1) and (age >= 16 and age <= 24)'},
        ]
        mr = ds.create_categorical(responses, alias='mr', name='my mr', multiple=True)
        ds.resource.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'alias': 'mr',
                'description': '',
                'name': 'my mr',
                'derivation': {
                    'function': 'array',
                    'args': [{
                        'function': 'select',
                        'args': [{
                            'map': {
                                '0001': {
                                    'function': 'case',
                                    'references': {
                                        'name': 'Facebook',
                                        'alias': 'mr_1',
                                    },
                                    'args': [{
                                        'column': [1, 2],
                                        'type': {
                                            'value': {
                                                'class': 'categorical',
                                                'categories': [
                                                    {'numeric_value': None, 'selected': True, 'id': 1, 'name': 'Selected', 'missing': False},
                                                    {'numeric_value': None, 'selected': False, 'id': 2, 'name': 'Not selected', 'missing': False}
                                                ]
                                            }
                                        }
                                    }, {
                                        # 'var_a > 5'
                                        'function': '>',
                                        'args': [
                                            {'variable': 'https://test.crunch.io/api/datasets/123456/variables/%s/' % variables['var_a']['id']},
                                            {'value': 5}
                                        ]
                                    }]
                                },
                                '0002': {
                                    'function': 'case',
                                    'references': {
                                        'alias': 'mr_2',
                                        'name': 'Twitter',
                                    },
                                    'args': [{
                                        'column': [1, 2],
                                        'type': {
                                            'value': {
                                                'class': 'categorical',
                                                'categories': [
                                                    {'numeric_value': None, 'selected': True, 'id': 1, 'name': 'Selected', 'missing': False},
                                                    {'numeric_value': None, 'selected': False, 'id': 2, 'name': 'Not selected', 'missing': False}
                                                ]
                                            }
                                        }
                                    }, {
                                        # 'var_b < 10 and var_c in (1, 2, 3)'}
                                        'function': 'and',
                                        'args': [{
                                            'function': '<',
                                            'args': [
                                                {'variable': 'https://test.crunch.io/api/datasets/123456/variables/%s/' % variables['var_b']['id']},
                                                {'value': 10}
                                            ]
                                        }, {
                                            'function': 'in',
                                            'args': [
                                                {'variable': 'https://test.crunch.io/api/datasets/123456/variables/%s/' % variables['var_c']['id']},
                                                {'value': [1, 2, 3]}
                                            ]
                                        }]
                                    }]
                                },
                                '0003': {
                                    'function': 'case',
                                    'references': {
                                        'alias': 'mr_3',
                                        'name': 'Google+',
                                    },
                                    'args': [{
                                        'column': [1, 2],
                                        'type': {
                                            'value': {
                                                'class': 'categorical',
                                                'categories': [
                                                    {'numeric_value': None, 'selected': True, 'id': 1, 'name': 'Selected', 'missing': False},
                                                    {'numeric_value': None, 'selected': False, 'id': 2, 'name': 'Not selected', 'missing': False}
                                                ]
                                            }
                                        }
                                    }, {
                                        # '(gender == 1) and (age >= 16 and age <= 24)'
                                        'function': 'and',
                                        'args': [{
                                            'function': '==',
                                            'args': [{'variable': 'https://test.crunch.io/api/datasets/123456/variables/%s/' % variables['gender']['id']}, {'value': 1}]
                                        }, {
                                            'function': 'and',
                                            'args': [{
                                                'function': '>=',
                                                'args': [{'variable': 'https://test.crunch.io/api/datasets/123456/variables/%s/' % variables['age']['id']}, {'value': 16}]
                                            }, {
                                                'function': '<=',
                                                'args': [{'variable': 'https://test.crunch.io/api/datasets/123456/variables/%s/' % variables['age']['id']}, {'value': 24}]
                                            }]
                                        }]
                                    }]
                                }
                            }
                        }]
                    }]
                }
            }
        })


class TestCopyVariable(TestCase):
    def test_base_variable(self):
        ds_res = mock.MagicMock()
        var_res = mock.MagicMock()
        var_res.entity.body = {'type': 'numeric'}
        var_res.entity.self = '/variable/url/'
        ds = Dataset(ds_res)
        var = Variable(var_res, ds_res)
        ds.copy_variable(var, name='copy', alias='copy')
        ds_res.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'alias': 'copy',
                'name': 'copy',
                'derivation': {
                    'function': 'copy_variable',
                    'args': [{'variable': '/variable/url/'}]
                }
            }
        })

    def test_derived_variable(self):
        ds_res = mock.MagicMock()
        var_res = mock.MagicMock()
        var_res.entity.body = {
            'type': 'multiple_response', 'derivation': {
                'function': 'array',
                'args': [{
                    'function': 'select',
                    'args': [{
                        'map': {
                            '00001': {
                                'function': 'combine_responses',
                                'args': [
                                    {'variable': '../original_variable'}
                                ]
                            }
                        }
                    }]
                }]
            }}
        var_res.entity.self = '/variable/url/'
        ds = Dataset(ds_res)
        var = Variable(var_res, ds_res)
        ds.copy_variable(var, name='copy', alias='copy')
        ds_res.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'alias': 'copy',
                'name': 'copy',
                'derivation': {
                    'function': 'array',
                    'args': [{
                        'function': 'select',
                        'args': [{
                            'map': {
                                '00001': {
                                    'function': 'combine_responses',
                                    'args': [
                                        # Look how the variable url got abs()ed
                                        {'variable': '/variable/original_variable'}
                                    ]
                                }
                            }
                        }]
                    }]
                }
            }
        })


def test_hide_unhide():
    ds_res = mock.MagicMock()
    var_res = mock.MagicMock()
    var = Variable(var_res, ds_res)
    var.hide()
    var_res.entity.edit.assert_called_with(discarded=True)
    var.unhide()
    var_res.entity.edit.assert_called_with(discarded=False)


class TestHierarchicalOrder(TestCase):

    ds_url = 'http://test.crunch.local/api/datasets/123/'

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
        variables = dict()
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
            _get_func = _build_get_func(var)
            _var_mock = mock.MagicMock()
            _var_mock.__getitem__.side_effect = _get_func
            _var_mock.get.side_effect = _get_func
            _var_mock.entity.self = var_url
            _var_mock.entity.body.__getitem__.side_effect = _get_func
            _var_mock.entity.body.get.side_effect = _get_func
            table['metadata'][var['id']] = _var_mock
            variables[var['id']] = _var_mock     # for .variables.by('id')
            variables[var['alias']] = _var_mock  # for .variables.by('alias')

        def _session_get(*args):
            if args[0] == '{}table/'.format(self.ds_url):
                return _CrunchPayload(table)
            elif args[0] == '{}variables/hier/'.format(self.ds_url):
                self.ds._hier_calls += 1
                return _CrunchPayload(hier_order)
            if args[0] == '{}state/'.format(self.ds_url):
                return _CrunchPayload({
                    'element': 'shoji:entity',
                    'self': '%sstate/' % self.ds_url,
                    'body': _CrunchPayload({
                        'revision': self.ds._revision
                    })
                })
            return _CrunchPayload()

        ds_resource = mock.MagicMock()
        ds_resource.self = self.ds_url
        ds_resource.variables.orders.hier = '%svariables/hier/' % self.ds_url
        ds_resource.variables.by.return_value = variables
        ds_resource.session.get.side_effect = _session_get
        self.ds = Dataset(ds_resource)
        self.ds._revision = 'one'
        self.ds._hier_calls = 0

    def test_order_property_is_loaded_correctly(self):
        ds = self.ds

        assert isinstance(ds.order, scrunch.datasets.Order)
        assert isinstance(ds.order.graph, scrunch.datasets.Group)  # root group

    def test_order_property_is_protected_from_modifications(self):
        ds = self.ds

        # The `order` property must be protected from modifications.
        with pytest.raises(TypeError):
            ds.order = False

        # The "root" Group must also be protected from modifications.
        with pytest.raises(TypeError):
            ds.order.graph = None

    def test_access_with_absolute_paths(self):
        ds = self.ds

        # The "root" Group.
        root_group = ds.order['|']
        assert isinstance(root_group, scrunch.datasets.Group)
        assert root_group.is_root

        # Sub-groups
        group = ds.order['|Account']
        assert isinstance(group, scrunch.datasets.Group)
        assert group.name == 'Account'
        assert group.parent == ds.order.graph

        group = ds.order['|Account|User Information|']
        assert isinstance(group, scrunch.datasets.Group)
        assert group.name == 'User Information'
        assert group.parent == ds.order['|Account']

        with pytest.raises(scrunch.exceptions.InvalidPathError):
            _ = ds.order['|Account|Invalid Group|']

        with pytest.raises(scrunch.exceptions.InvalidPathError):
            _ = ds.order['|Invalid Group|']

    def test_access_with_relative_paths(self):
        ds = self.ds

        acct_group = ds.order['Account']
        assert isinstance(acct_group, scrunch.datasets.Group)
        assert acct_group.name == 'Account'
        assert acct_group.parent == ds.order.graph

        usr_info_group = acct_group['User Information']
        assert isinstance(usr_info_group, scrunch.datasets.Group)
        assert usr_info_group.name == 'User Information'
        assert usr_info_group.parent == acct_group

        with pytest.raises(scrunch.exceptions.InvalidPathError):
            _ = ds.order['Invalid Group']

        with pytest.raises(scrunch.exceptions.InvalidPathError):
            _ = acct_group['Another Invalid Group']

    def test_access_with_the_in_operator(self):
        ds = self.ds

        assert 'Account' in ds.order['|']
        assert 'Invalid Group' not in ds.order['|']
        assert 'User Information' in ds.order['|Account']

    def test_element_str_representation(self):
        ds = self.ds

        # Test first-level str representation.
        assert str(ds.order) == json.dumps(
            [
                'ID',
                'Hobbies',
                {
                    'Account': [
                        'Registration Time',
                        'Last Login Time',
                        {
                            'User Information': [
                                'First Name',
                                'Last Name',
                                'Gender'
                            ]
                        },
                        {
                            'Location': [
                                'Country',
                                'City',
                                'Zip Code',
                                'Address'
                            ]
                        }
                    ]
                },
                'Music',
                'Religion'
            ],
            indent=scrunch.datasets.Group.INDENT_SIZE
        )

        # Test sub-group str representation.
        assert str(ds.order['|Account']) == json.dumps(
            [
                'Registration Time',
                'Last Login Time',
                {
                    'User Information': [
                        'First Name',
                        'Last Name',
                        'Gender'
                    ]
                },
                {
                    'Location': [
                        'Country',
                        'City',
                        'Zip Code',
                        'Address'
                    ]
                }
            ],
            indent=scrunch.datasets.Group.INDENT_SIZE
        )

        assert str(ds.order['|Account|User Information']) == json.dumps(
            [
                'First Name',
                'Last Name',
                'Gender'
            ],
            indent=scrunch.datasets.Group.INDENT_SIZE
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

        ds.order['|'].append('id')
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

        ds.order['|'].insert('music', position=1)
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

        ds.order['|'].insert(alias=['id', 'Account'], position=3)
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

        ds.order['|'].insert(['Account', 'id'])
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

        ds.order['|'].append('hobbies')
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

        ds.order['|'].insert(['hobbies', 'religion'], before='music')
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

        ds.order['|'].insert('Account', after='id')
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

    def test_local_movements_using_paths(self):
        ds = self.ds

        ds.order['|Account'].append('registration_time')
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

        ds.order['|Account|User Information'].insert('gender', position=1)
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

        ds.order['Account|Location'].insert(['country', 'zip_code'], 2)
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

        ds.order['|Account|Location'].insert('address')
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

        ds.order['|Account'].append('User Information')
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

        ds.order['|Account'].insert('Location', before='last_login_time')
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

        ds.order['|Account|Location'].insert('city', after='country')
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

    def test_cross_group_movements(self):
        ds = self.ds

        ds.order['|'].append('gender')
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

        ds.order['|'].insert('address', position=1)
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

        ds.order['|'].insert(alias=['last_login_time', 'Location'], position=3)
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

        ds.order['|'].insert(['User Information', 'country'])
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

        ds.order['|'].append('zip_code')
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

        ds.order['Account'].append('last_login_time')
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

        ds.order['|User Information'].append('gender')
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

        ds.order['|Account'].append(['User Information', 'Location'])
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

        ds.order['|Account|Location'].insert('address')
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

        ds.order['|Account|Location|'].insert('country', after='city')
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

        ds.order['|'].reorder(['id', 'hobbies', 'music', 'religion', 'Account'])
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

        ds.order['|Account'].reorder([
            'User Information',
            'Location',
            'registration_time',
            'last_login_time'
        ])
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

    def test_movement_errors(self):
        ds = self.ds

        with pytest.raises(ValueError):
            ds.order['|Account|User Information'].append('invalid_alias')

        with pytest.raises(ValueError):
            ds.order['|Account'].insert(alias=['id', 'invalid_alias'])

        with pytest.raises(IndexError):
            ds.order['|Account'].insert('gender', position=999)

        with pytest.raises(TypeError):
            ds.order['|Account'].insert('id', before=1)

        with pytest.raises(scrunch.exceptions.InvalidReferenceError):
            ds.order['|Account'].insert('id', before='unknown')

    def test_group_creation(self):
        ds = self.ds

        ds.order['|Account'].create_group(
            'Login Details', alias=['registration_time', 'last_login_time']
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
            ds.order['|'].create_group('Account')

        with pytest.raises(ValueError):
            ds.order['|'].create_group('My new|Group')

        with pytest.raises(ValueError):
            ds.order['|'].create_group(
                'Very very very long name for the new Group which should not '
                'be allowed at all'
            )

        with pytest.raises(ValueError):
            ds.order['|Account'].create_group('@##$')

    def test_group_renaming(self):
        ds = self.ds

        ds.order['|Account|User Information'].rename('User Info')
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

        with pytest.raises(ValueError):
            ds.order['|'].rename('Root')

        with pytest.raises(ValueError):
            ds.order['|Account'].rename('id')

        with pytest.raises(ValueError):
            ds.order['|Account'].rename('My new|Group')

        with pytest.raises(ValueError):
            ds.order['|Account'].rename(
                'Very very very long new name for the Group which should not '
                'be allowed at all'
            )

        with pytest.raises(ValueError):
            ds.order['|Account'].rename('@##$.')

    def test_move_group(self):
        ds = self.ds

        group = ds.order['|Account|User Information']
        assert group.name == 'User Information'
        group.move('|')
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
                {
                    'User Information': [
                        '../000005/',       # first_name
                        '../000006/',       # last_name
                        '../000007/',       # gender
                    ]
                },
            ]
        }

        with pytest.raises(scrunch.exceptions.InvalidPathError):
            ds.order['|Account|Location'].move('|Invalid Group|')

        with pytest.raises(scrunch.exceptions.InvalidPathError):
            ds.order['|Account|Location'].move('|Account|Location')

    def test_move_variable(self):
        ds = self.ds
        var = ds['id']
        assert var.name == 'ID'
        var.move('|Account|User Information')
        assert self._get_update_payload(var.dataset) == {
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
                                '../000007/',       # gender
                                '../000001/',       # id
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

        with pytest.raises(scrunch.exceptions.InvalidPathError):
            var.move('|Account|Invalid Group')

    def test_order_synchronization(self):
        ds = self.ds

        # Only one call to the hierarchical order endpoint should be done as
        # long as the dataset revision doesn't change. More details at
        # .setUp().
        assert isinstance(ds.order['|'], scrunch.datasets.Group)
        assert isinstance(ds.order['|Account'], scrunch.datasets.Group)
        assert isinstance(ds.order['|'], scrunch.datasets.Group)
        assert ds._hier_calls == 1

        # Simulate the dataset having a new revision so that the
        # synchronization mechanism kicks in. More details at .setUp().
        ds._revision = 'two'
        assert isinstance(ds.order['|'], scrunch.datasets.Group)
        assert ds._hier_calls == 2


class TestDatasetSettings(TestCase):

    ds_url = 'http://test.crunch.local/api/datasets/123/'

    def setUp(self):
        settings = {
            'element': 'shoji:entity',
            'self': '%ssettings/' % self.ds_url,
            'body': {
                'viewers_can_export': False,
                'min_base_size': 0,
                'weight': None,
                'viewers_can_change_weight': False
            }
        }

        def _session_get(*args):
            if args[0] == '{}settings/'.format(self.ds_url):
                return _CrunchPayload(settings)
            return _CrunchPayload()

        ds_resource = mock.MagicMock()
        ds_resource.self = self.ds_url
        ds_resource.fragments.settings = '%ssettings/' % self.ds_url
        ds_resource.session.get.side_effect = _session_get
        self.ds = Dataset(ds_resource)

    def test_settings_are_displayed_as_dict_obj(self):
        ds = self.ds

        assert isinstance(ds.settings, dict)
        assert ds.settings == {
            'viewers_can_export': False,
            'min_base_size': 0,
            'weight': None,
            'viewers_can_change_weight': False
        }

    def test_settings_obj_is_protected_from_modifications(self):
        ds = self.ds

        # The `settings` property must be protected from modifications.
        with pytest.raises(TypeError):
            ds.settings = False

    def test_settings_dict_obj_is_read_only(self):
        ds = self.ds

        with pytest.raises(RuntimeError):
            ds.settings['viewers_can_export'] = 'invalid'

        with pytest.raises(RuntimeError):
            del ds.settings['viewers_can_export']

        with pytest.raises(RuntimeError):
            ds.settings.pop()

        with pytest.raises(RuntimeError):
            ds.settings.update({'viewers_can_export': 'invalid'})

        with pytest.raises(RuntimeError):
            ds.settings.clear()

    def test_change_settings(self):
        ds = self.ds

        # Test that the change_settings method performs the proper PATCHes.
        ds.change_settings(viewers_can_export=True)
        _url = ds.session.patch.call_args_list[-1][0][0]
        _payload = json.loads(ds.session.patch.call_args_list[-1][0][1])
        _headers = ds.session.patch.call_args_list[-1][1].get('headers', {})
        assert _url == self.ds_url + 'settings/'
        assert _payload == {'viewers_can_export': True}
        assert _headers == {'Content-Type': 'application/json'}

        ds.change_settings(
            viewers_can_export=True, viewers_can_change_weight=True
        )
        _url = ds.session.patch.call_args_list[-1][0][0]
        _payload = json.loads(ds.session.patch.call_args_list[-1][0][1])
        _headers = ds.session.patch.call_args_list[-1][1].get('headers', {})
        assert _url == self.ds_url + 'settings/'
        assert _payload == {
            'viewers_can_export': True,
            'viewers_can_change_weight': True
        }
        assert _headers == {'Content-Type': 'application/json'}

        # Test that trying to edit invalid or read-only settings is forbidden.
        with pytest.raises(ValueError):
            ds.change_settings(invalid_setting=True)
        with pytest.raises(ValueError):
            ds.change_settings(viewers_can_export=True, weight=10)


class TestDatasetJoins(TestCase):
    left_ds_url = 'https://test.crunch.io/api/datasets/123/'
    right_ds_url = 'https://test.crunch.io/api/datasets/456/'

    def _variable_mock(self, ds_url, var):
        var_url = '%svariables/%s/' % (ds_url, var['id'])
        _get_func = _build_get_func(var)
        _var_mock = mock.MagicMock()
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func
        _var_mock.entity.self = var_url
        _var_mock.entity.body.__getitem__.side_effect = _get_func
        _var_mock.entity.body.get.side_effect = _get_func
        return _var_mock

    def setUp(self):
        var = {
            'id': '000001',
            'alias': 'id',
            'name': 'ID',
            'type': 'numeric',
            'is_subvar': False
        }

        # setup for left dataset
        _left_var_mock = self._variable_mock(self.left_ds_url, var)
        left_variable = collections.OrderedDict()
        left_variable[var['alias']] = _left_var_mock
        left_ds_res = mock.MagicMock()
        left_ds_res.self = self.left_ds_url
        left_ds_res.variables.by.return_value = left_variable
        self.left_ds = Dataset(left_ds_res)

        # setup for right dataset
        _right_var_mock = self._variable_mock(self.right_ds_url, var)
        right_variable = collections.OrderedDict()
        right_variable[var['alias']] = _right_var_mock
        right_ds_res = mock.MagicMock()
        right_ds_res.self = self.right_ds_url
        right_ds_res.variables.by.return_value = right_variable
        self.right_ds = Dataset(right_ds_res)

    def test_dataset_joins(self):
        left_ds = self.left_ds
        right_ds = self.right_ds
        left_var = left_ds['id']
        right_var = right_ds['id']

        left_ds.join('id', right_ds, 'id', wait=False)

        call_payload = left_ds.resource.variables.post.call_args[0][0]
        expected_payload = {
            'element': 'shoji:entity',
            'body': {
                'function': 'adapt',
                'args': [
                    {'dataset': right_ds.url},
                    {'variable': '%svariables/%s/' % (right_ds.url, right_var.id)},
                    {'variable': '%svariables/%s/' % (left_ds.url, left_var.id)}
                ]
            }
        }

        assert call_payload == expected_payload
        left_ds.resource.variables.post.assert_called_once_with(
            expected_payload)
