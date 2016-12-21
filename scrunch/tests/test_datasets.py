import json

import mock
from unittest import TestCase

import pytest
from pandas import DataFrame
from scrunch.datasets import Dataset, Variable
from pycrunch.elements import JSONObject
from pycrunch.shoji import Entity
from pycrunch.variables import cast


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
            {'id': 1, 'name': 'Facebook', 'rules': 'var_a > 5'},
            {'id': 2, 'name': 'Twitter',
             'rules': 'var_b < 10 and var_c in (1, 2, 3)'},
            {'id': 3, 'name': 'Google+',
             'rules': '(gender == 1) and (age >= 16 and age <= 24)'},
        ]
        ds.recode(responses, alias='cat', name='My cat', multiple=False)
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
            {'id': 1, 'name': 'Facebook', 'rules': 'var_a > 5'},
            {'id': 2, 'name': 'Twitter', 'rules': 'var_b < 10 and var_c in (1, 2, 3)'},
            {'id': 3, 'name': 'Google+', 'rules': '(gender == 1) and (age >= 16 and age <= 24)'},
        ]
        mr = ds.recode(responses, alias='mr', name='my mr', multiple=True)
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


class TestEditCombination(TestCase):
    def test_edit_single_response(self):
        var_res = mock.MagicMock()
        var_res.body.derivation = {
            'function': 'combine_categories',
            'args': [{'variable': 'parent_url'}]
        }
        variable = Variable(var_res)
        variable.edit_combination([
            {'id': 1, 'name': 'editing', 'combined_ids': [1, 2, 3], 'missing': False}
        ])
        var_res.edit.assert_called_with(derivation={
            'function': 'combine_categories',
            'args': [
                {'variable': 'parent_url'},
                {'value': [{'combined_ids': [1, 2, 3], 'missing': False, 'id': 1, 'name': 'editing'}]}
            ]
        })

    def test_edit_multiple_response(self):
        var_res = mock.MagicMock()
        parent_mock = mock.MagicMock()
        subvar_mock = mock.MagicMock(entity_url='subvar_url')
        parent_mock.entity.body = {'alias': 'parent_alias'}
        parent_mock.entity.subvariables.by.return_value = {
            'parent_alias_1': subvar_mock,
            'parent_alias_2': subvar_mock,
        }
        var_res.parent.by.return_value = {
            'original_variable': parent_mock
        }
        var_res.body.derivation = {
            'function': 'array',
            'args': [{
                'function': 'select',
                'args': [{
                    'map': {
                        '00001': {
                            'function': 'combine_responses',
                            'args': [
                                {'variable': 'original_variable'}
                            ]
                        }
                    }
                }]
            }]
        }
        variable = Variable(var_res)
        variable.type = 'multiple_response'
        variable.alias = 'mr_alias'
        variable.edit_combination([
            {'id': 1, 'name': 'editing', 'combined_ids': [1, 2], 'missing': False}
        ])
        var_res.edit.assert_called_with(derivation={
            'function': 'combine_responses',
            'args': [
                {'variable': 'original_variable'},
                {'value': [
                    {'alias': 'mr_alias_1', 'combined_ids': ['subvar_url', 'subvar_url'], 'name': 'editing'}
                ]}
            ]
        })


class TestCopyVariable(TestCase):
    def test_base_variable(self):
        ds_res = mock.MagicMock()
        var_res = mock.MagicMock(body={'type': 'numeric'})
        var_res.self = '/variable/url/'
        ds = Dataset(ds_res)
        var = Variable(var_res)
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
        var_res = mock.MagicMock(body={'type': 'multiple_response', 'derivation': {
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
        }})
        var_res.self = '/variable/url/'
        ds = Dataset(ds_res)
        var = Variable(var_res)
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

