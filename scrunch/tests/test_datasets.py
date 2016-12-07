import json

import mock
from unittest import TestCase

import pytest
from pandas import DataFrame
from scrunch.datasets import Dataset
from pycrunch.elements import JSONObject
from pycrunch.shoji import Entity
from pycrunch.variables import cast


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

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.exclusion = '%sexclusion/' % self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.__class__ = Dataset
        ds.exclude = Dataset.exclude
        ds.session.get.side_effect = _session_get
        ds = Dataset(ds)

        # Action!
        exclusion_filter = 'disposition != 0'
        ds.exclude(exclusion_filter)

        # Ensure .patch was called the right way.
        assert len(ds.session.patch.call_args_list) == 1

        call = ds.session.patch.call_args_list[0]
        assert call[0][0] == ds.fragments.exclusion

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
        ds = mock.MagicMock()
        ds.fragments.exclusion = '%sexclusion/' % self.ds_url
        ds.__class__ = Dataset
        ds.exclude = Dataset.exclude
        ds.exclude(ds)

        ds.session.patch.assert_called_once_with(
            ds.fragments.exclusion,
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

    def test_has_any(self):
        data = self._exclude_payload('exit_status.has_any([32766])')
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

    def test_not_has_any(self):
        data = self._exclude_payload('not exit_status.has_any([32766])')
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

    def test_has_any_multiple(self):
        data = self._exclude_payload('exit_status.has_any([32766, 32767])')
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

    def test_has_all(self):
        data = self._exclude_payload('exit_status.has_all([32767])')
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

    def test_not_has_all(self):
        data = self._exclude_payload('not exit_status.has_all([32767])')
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

    def test_has_all_or_has_all(self):
        data = self._exclude_payload('exit_status.has_all([1]) or exit_status.has_all([2])')
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

    def test_not_has_all_or_has_all(self):
        data = self._exclude_payload('not(exit_status.has_all([1]) or exit_status.has_all([2]))')
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

        test_variable = mock.MagicMock()
        test_variable.entity = Entity(session=session)

        variables = {
            'test_variable': test_variable
        }
        dataset = Dataset({})
        dataset.variables = mock.MagicMock()
        dataset.variables.by.return_value = variables

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
        with pytest.raises(KeyError) as err:
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
        with pytest.raises(KeyError) as err:
            ds.create_savepoint('savepoint description')

    def test_load_empty_savepoint(self):
        sess = mock.MagicMock()
        ds_res = mock.MagicMock(session=sess)
        ds_res.savepoints = mock.MagicMock()
        ds_res.savepoints.index = {}
        ds = Dataset(ds_res)
        with pytest.raises(KeyError) as err:
            ds.load_savepoint('savepoint')


class TestForks(TestCase):

    ds_url = 'http://test.crunch.io/api/datasets/123/'

    def test_fork(self):
        sess = mock.MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'description': 'ds description'
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
        f = ds.fork(preserve_owner=True)
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

        f1.entity.delete.call_count == 1
        f2.entity.delete.call_count == 1
        f3.entity.delete.call_count == 1

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