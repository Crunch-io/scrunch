import collections
import json
import copy

import mock
from mock import MagicMock
from unittest import TestCase

import pytest
from pandas import DataFrame
from pycrunch.elements import JSONObject, ElementSession
from pycrunch.variables import cast

import scrunch
from scrunch.datasets import (Dataset, Variable,
                              User, Project, Filter)
from scrunch.tests.test_categories import EditableMock, TEST_CATEGORIES


class _CrunchPayload(dict):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.put = MagicMock()
        self.post = MagicMock()
        self.patch = MagicMock()

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
    api = 'https://test.crunch.io/api/'

    ds_shoji = {
        'element': 'shoji:entity',
        'body': {
            'id': '123456',
            'name': 'test_dataset_name',
            'notes': '',
            'description': '',
            'is_published': False,
            'archived': False,
            'end_date': None,
            'start_date': None,
        },
    }

    variables = {
        '0001': dict(
            id='0001',
            alias='var1_alias',
            name='var1_name',
            description='',
            notes='',
            format=None,
            view=None,
            type='numeric',
            is_subvar=False
        ),
        '0002': dict(
            id='0002',
            alias='var2_alias',
            name='var2_name',
            description='',
            notes='',
            format=None,
            view=None,
            type='text',
            is_subvar=False
        ),
        '0003': dict(
            id='0003',
            alias='var3_alias',
            name='var3_name',
            description=None,
            notes=None,
            format=None,
            view=None,
            type='categorical',
            categories=TEST_CATEGORIES(),
            is_subvar=False
        )
    }

    def _dataset_mock(self, ds_shoji=None, variables=None):
        ds_shoji = ds_shoji or copy.deepcopy(self.ds_shoji)
        ds_url = '%sdatasets/%s/' % (self.api, ds_shoji['body']['id'])
        variables = variables or copy.deepcopy(self.variables)

        table, _variables = self._build_test_meta(ds_shoji, variables)
        ds_shoji['body']['table'] = table

        var_mock_attributes = {
            'by.side_effect': self._variables_by_side_effect(_variables)
        }

        ds_mock_attributes = {
            'body': ds_shoji['body'],
            'variables': MagicMock(**var_mock_attributes),
            'session': MagicMock(spec=ElementSession),
            'fragments.exclusion': '%sexclusion/' % ds_url
        }
        _ds_mock = EditableMock(**ds_mock_attributes)
        _ds_mock.self = ds_url

        table_mock = MagicMock(metadata=variables)
        table_mock.self = table.get('self')
        _ds_mock.follow.return_value = table_mock
        return _ds_mock

    def _variable_mock(self, ds_url, variable=None):
        variable = variable or self.variables['0001']
        var_url = '%svariables/%s/' % (ds_url, variable['id'])
        # set attrs outside of entity
        _var_mock = MagicMock(variable)
        _var_mock.entity = EditableMock(body=variable)
        _var_mock.entity.self = var_url
        # force new Variable behaviour not being an entity
        _var_mock.entity_url = var_url
        return _var_mock

    def _build_test_meta(self, ds_shoji, variables):
        ds_url = '%sdatasets/%s/' % (self.api, ds_shoji['body']['id'])
        table = dict(
            element='crunch:table',
            self='%stable/' % ds_url,
            metadata=collections.OrderedDict()
        )

        _variables = dict(id=dict(), name=dict(), alias=dict())
        for var in variables:
            _var_mock = self._variable_mock(ds_url, variables[var])
            _variables['id'].update({variables[var]['id']: _var_mock})
            _variables['name'].update({variables[var]['name']: _var_mock})
            _variables['alias'].update({variables[var]['alias']: _var_mock})
            table['metadata'][variables[var]['id']] = _var_mock

        return table, _variables

    def _variables_by_side_effect(self, variables):
        _variables = variables

        def _get(*args):
            return _variables.get(args[0])
        return _get


class TestDatasets(TestDatasetBase, TestCase):

    def test_edit_Dataset(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)

        assert ds.name == 'test_dataset_name'
        changes = dict(name='changed')
        ds.edit(**changes)
        assert ds.name == 'changed'
        ds.resource._edit.assert_called_with(**changes)

        assert ds.description == ''
        changes = dict(description='changed')
        ds.edit(**changes)
        assert ds.description == 'changed'
        ds.resource._edit.assert_called_with(**changes)

        assert ds.notes == ''
        changes = dict(notes='changed')
        ds.edit(**changes)
        assert ds.notes == 'changed'
        ds.resource._edit.assert_called_with(**changes)

        assert ds.is_published is False
        changes = dict(is_published=True)
        ds.edit(**changes)
        assert ds.is_published is True
        ds.resource._edit.assert_called_with(**changes)

        assert ds.archived is False
        changes = dict(archived=True)
        ds.edit(**changes)
        assert ds.archived is True
        ds.resource._edit.assert_called_with(**changes)

        assert ds.end_date is None
        changes = dict(end_date='2017-01-01')
        ds.edit(**changes)
        assert ds.end_date == '2017-01-01'
        ds.resource._edit.assert_called_with(**changes)

        assert ds.start_date is None
        changes = dict(start_date='2017-01-01')
        ds.edit(**changes)
        assert ds.start_date == '2017-01-01'
        ds.resource._edit.assert_called_with(**changes)


class TestExclusionFilters(TestDatasetBase, TestCase):

    def test_apply_exclusion(self):
        """
        Tests that the proper PATCH request is sent to Crunch in order to
        apply an exclusion filter to a dataset.
        """
        ds_res = self._dataset_mock()
        ds = Dataset(ds_res)
        var = ds['var1_alias']

        # Action!
        exclusion_filter = 'var1_alias != 0'
        ds.exclude(exclusion_filter)

        # Ensure .patch was called the right way.
        assert len(ds.resource.session.patch.call_args_list) == 1

        call = ds.resource.session.patch.call_args_list[0]
        assert call[0][0] == ds.resource.fragments.exclusion

        expected_expr_obj = {
            'expression': {
                'function': '!=',
                'args': [
                    {'variable': var.url},  # Crunch needs variable URLs!
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
        ds_res = MagicMock()
        ds = Dataset(ds_res)
        ds.exclude()

        ds.resource.session.patch.assert_called_once_with(
            ds.resource.fragments.exclusion,
            data=json.dumps({'expression': {}})
        )

    def _exclude_payload(self, ds, expr):
        ds.exclude(expr)
        call = ds.resource.session.patch.call_args_list[0]
        return json.loads(call[1]['data'])

    def test_gt(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias > 5')
        expected_expr_obj = {
            'expression': {
                'function': '>',
                'args': [
                    {'variable': var.url},
                    {'value': 5}
                ]
            }
        }
        assert data == expected_expr_obj

    def test_in(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias in [32766]')
        expected_expr_obj = {
            "expression": {
                "function": "in",
                "args": [
                    {"variable": var.url},
                    {"value": [32766]}
                ]
            }
        }

        assert data == expected_expr_obj

    def test_in_multiple(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias in (32766, 32767)')
        expected_expr_obj = {
            "expression": {
                "function": "in",
                "args": [
                    {"variable": var.url},
                    {"value": [32766, 32767]}
                ]
            }
        }

        assert data == expected_expr_obj

    def test_not_and(self):
        variables = {
            '0001': dict(
                id='0001',
                alias='disposition',
                name='Disposition',
                type='numeric'
            ),
            '0002': dict(
                id='0002',
                alias='exit_status',
                name='Exit',
                type='numeric'
            )
        }

        ds_mock = self._dataset_mock(variables=variables)
        ds = Dataset(ds_mock)
        var1 = ds['disposition']
        var2 = ds['exit_status']

        data = self._exclude_payload(ds, 'not (disposition in (1, 2) and exit_status == 0)')
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
                                        "variable": var1.url
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
                                        "variable": var2.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias.any([32766])')
        expected_expr_obj = {
            "expression": {
                "function": "any",
                "args": [
                    {
                        "variable": var.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'not var1_alias.any([32766])')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "function": "any",
                        "args": [
                            {
                                "variable": var.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias.any([32766, 32767])')
        expected_expr_obj = {
            "expression": {
                "function": "any",
                "args": [
                    {
                        "variable": var.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias.all([32767])')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "variable": var.url
                    },
                    {
                        "value": [32767]
                    }
                ],
                "function": "all"
            }
        }

        assert data == expected_expr_obj

    def test_not_all(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'not var1_alias.all([32767])')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "function": "all",
                        "args": [
                            {
                                "variable": var.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias.all([1]) or var1_alias.all([2])')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "args": [
                            {
                                "variable": var.url
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
                                "variable": var.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'not(var1_alias.all([1]) or var1_alias.all([2]))')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "args": [
                            {
                                "args": [
                                    {
                                        "variable": var.url
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
                                        "variable": var.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias.duplicates()')
        expected_expr_obj = {
            "expression": {
                "function": "duplicates",
                "args": [
                    {
                        "variable": var.url
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_valid(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'valid(var1_alias)')
        expected_expr_obj = {
            "expression": {
                "function": "is_valid",
                "args": [
                    {
                        "variable": var.url
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_not_valid(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'not valid(var1_alias)')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "args": [
                            {
                                "variable": var.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'missing(var1_alias)')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "variable": var.url
                    }
                ],
                "function": "is_missing"
            }
        }

        assert data == expected_expr_obj

    def test_not_missing(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'not missing(var1_alias)')
        expected_expr_obj = {
            "expression": {
                "function": "not",
                "args": [
                    {
                        "function": "is_missing",
                        "args": [
                            {
                                "variable": var.url
                            }
                        ]
                    }
                ]
            }
        }

        assert data == expected_expr_obj

    def test_equal(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        data = self._exclude_payload(ds, 'var1_alias == 1')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "variable": var.url
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
        variables = {
            '0001': dict(
                id='0001',
                alias='disposition',
                name='Disposition',
                type='numeric'
            ),
            '0002': dict(
                id='0002',
                alias='exit_status',
                name='Exit',
                type='numeric'
            )
        }
        ds_mock = self._dataset_mock(variables=variables)
        ds = Dataset(ds_mock)
        var1 = ds['disposition']
        var2 = ds['exit_status']

        data = self._exclude_payload(ds, '(disposition != 1 and (not valid(exit_status) or exit_status >= 1)) or (disposition == 0 and exit_status == 0) or (disposition == 0 and exit_status == 1)')
        expected_expr_obj = {
            "expression": {
                "args": [
                    {
                        "args": [
                            {
                                "args": [
                                    {
                                        "variable": var1.url
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
                                                        "variable": var2.url
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
                                                "variable": var2.url
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
                                                "variable": var1.url
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
                                                "variable": var2.url
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
                                                "variable": var1.url
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
                                                "variable": var2.url
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
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)

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
        data = self._exclude_payload(ds, expr)
        expected_expr_obj = {'expression': expr}
        assert data == expected_expr_obj


class TestProtectAttributes(TestDatasetBase, TestCase):
    error_msg = 'use the edit() method for mutating attributes'

    def test_Dataset_attribute_writes(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        assert ds.name == 'test_dataset_name'

        with pytest.raises(AttributeError, message=self.error_msg):
            ds.name = 'forbidden'
        assert ds.name == 'test_dataset_name'

        with pytest.raises(AttributeError, message=self.error_msg):
            ds.notes = 'forbidden'
        assert ds.notes == ''

        with pytest.raises(AttributeError, message=self.error_msg):
            ds.description = 'forbidden'
        assert ds.description == ''

        with pytest.raises(AttributeError, message=self.error_msg):
            ds.is_published = True
        assert ds.is_published is False

        with pytest.raises(AttributeError, message=self.error_msg):
            ds.archived = True
        assert ds.archived is False

        with pytest.raises(AttributeError, message=self.error_msg):
            ds.end_date = 'forbidden'
        assert ds.end_date is None

        with pytest.raises(AttributeError, message=self.error_msg):
            ds.start_date = 'forbidden'
        assert ds.start_date is None

    def test_Variable_attribute_writes(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        with pytest.raises(AttributeError, message=self.error_msg):
            var.name = 'forbidden'
        assert var.name == 'var1_name'

        with pytest.raises(AttributeError, message=self.error_msg):
            var.description = 'forbidden'
        assert var.description == ''

        with pytest.raises(AttributeError, message=self.error_msg):
            var.notes = 'forbidden'
        assert var.notes == ''

        with pytest.raises(AttributeError, message=self.error_msg):
            var.format = 'forbidden'
        assert var.format is None

        with pytest.raises(AttributeError, message=self.error_msg):
            var.view = 'forbidden'
        assert var.view is None


class TestVariables(TestDatasetBase, TestCase):
    def test_variable_as_member(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        assert ds.name == self.ds_shoji['body']['name']
        assert ds.id == self.ds_shoji['body']['id']

        assert isinstance(ds['var1_alias'], Variable)

        with pytest.raises(ValueError) as err:
            ds['some_variable']
        assert str(err.value) == \
            'Dataset %s has no variable with an alias some_variable' % ds.name

        with pytest.raises(AttributeError) as err:
            ds.some_variable
        assert str(err.value) == \
               "'Dataset' object has no attribute 'some_variable'"

    def test_variable_cast(self):
        variable = MagicMock()
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

    def test_edit_Variables(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        var = ds['var1_alias']

        assert var.name == 'var1_name'
        changes = dict(name='changed')
        var.edit(**changes)
        assert var.name == 'changed'
        var.resource._edit.assert_called_with(**changes)

        assert var.description == ''
        changes = dict(description='changed')
        var.edit(**changes)
        assert var.description == 'changed'
        var.resource._edit.assert_called_with(**changes)

        assert var.notes == ''
        changes = dict(notes='changed')
        var.edit(**changes)
        assert var.notes == 'changed'
        var.resource._edit.assert_called_with(**changes)

        assert var.format is None
        changes = dict(format=dict(summary=dict(digits=2)))
        var.edit(**changes)
        assert var.format == dict(summary=dict(digits=2))
        var.resource._edit.assert_called_with(**changes)

        assert var.view is None
        changes = dict(view=dict(show_counts=True))
        var.edit(**changes)
        assert var.view == dict(show_counts=True)
        var.resource._edit.assert_called_with(**changes)


class TestCurrentEditor(TestDatasetBase, TestCase):
    ds_url = 'https://test.crunch.io/api/datasets/123456/'
    user_url = 'https://test.crunch.io/api/users/12345/'

    def test_change_editor(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        ds.change_editor(self.user_url)

        ds_mock.patch.assert_called_with({
            'current_editor': self.user_url
        })

    def test_change_editor_email(self):
        sess = MagicMock()
        response = MagicMock()
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
        ds_res = MagicMock(session=sess)
        ds_res.self = self.ds_url
        ds_res.patch = MagicMock()
        ds = Dataset(ds_res)
        ds.change_editor('jane.doe@crunch.io')

        ds_res.patch.assert_called_with({
            'current_editor': self.user_url
        })


class TestCurrentOwner(TestDatasetBase, TestCase):
    user_url = 'https://test.crunch.io/api/users/12345/'
    user_email = 'test@crunch.com'
    project_url = 'https://test.crunch.io/api/projects/12345/'

    def test_change_owner_exception(self):
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        with pytest.raises(AttributeError) as e:
            ds.change_owner(user=self.user_url, project=self.project_url)
            assert e.message == "Must provide user or project. Not both"

    @mock.patch('scrunch.datasets.get_user')
    def test_change_owner(self, mocked_get_user):
        user = MagicMock()
        user.resource.self = self.user_url
        user.url = self.user_url
        mocked_get_user.return_value = user
        ds_mock = self._dataset_mock()
        ds = Dataset(ds_mock)
        ds.change_owner(user=user)
        ds_mock.patch.assert_called_with({
            'owner': self.user_url
        })


class TestSavepoints(TestCase):

    ds_url = 'http://test.crunch.io/api/datasets/123/'

    def test_create_savepoint(self):
        sess = MagicMock()
        ds_res = MagicMock(session=sess)
        ds_res.savepoints = MagicMock()
        ds = Dataset(ds_res)
        ds.create_savepoint('savepoint description')
        ds_res.savepoints.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'description': 'savepoint description'
            }
        })

    def test_create_savepoint_keyerror(self):
        sess = MagicMock()
        ds_res = MagicMock(session=sess)
        ds_res.savepoints = MagicMock()
        ds_res.savepoints.index = {
            1: {
                'description': 'savepoint description'
            }
        }
        ds = Dataset(ds_res)
        with pytest.raises(KeyError):
            ds.create_savepoint('savepoint description')

    def test_load_initial_savepoint(self):
        sess = MagicMock()
        ds_res = MagicMock(session=sess)
        ds_res.savepoints = MagicMock()
        ds_res.savepoints.index = {
            1: {
                'description': 'savepoint description'
            }
        }
        ds = Dataset(ds_res)
        with pytest.raises(KeyError):
            ds.create_savepoint('savepoint description')

    def test_load_empty_savepoint(self):
        sess = MagicMock()
        ds_res = MagicMock(session=sess)
        ds_res.savepoints = MagicMock()
        ds_res.savepoints.index = {}
        ds = Dataset(ds_res)
        with pytest.raises(KeyError):
            ds.load_savepoint('savepoint')


class TestForks(TestCase):

    ds_url = 'http://test.crunch.io/api/datasets/123/'

    def test_fork(self):
        sess = MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'description': 'ds description',
            'owner': 'http://test.crunch.io/api/users/123/'
        })
        ds_res = MagicMock(session=sess, body=body)
        ds_res.forks = MagicMock()
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

    def test_fork_preserve_owner(self):
        user_id = 'http://test.crunch.io/api/users/123/'
        sess = MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'description': 'ds description',
            'owner': user_id
        })
        ds_res = MagicMock(session=sess, body=body)
        ds_res.forks = MagicMock()
        ds_res.forks.index = {}
        ds = Dataset(ds_res)
        f = ds.fork(preserve_owner=True)
        f.resource.patch.assert_called_with({'owner': user_id})

    def test_fork_preserve_owner_project(self):
        project_id = 'http://test.crunch.io/api/projects/456/'
        sess = MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'description': 'ds description',
            'owner': project_id
        })
        ds_res = MagicMock(session=sess, body=body)
        ds_res.forks = MagicMock()
        ds_res.forks.index = {}
        ds = Dataset(ds_res)
        f = ds.fork()
        f.resource.patch.assert_called_with({'owner': project_id})

    def test_delete_forks(self):
        f1 = MagicMock()
        f2 = MagicMock()
        f3 = MagicMock()
        sess = MagicMock()
        ds_res = MagicMock(session=sess)
        ds_res.forks = MagicMock()
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
        sess = MagicMock()
        ds_res = MagicMock(session=sess)
        ds_res.forks = MagicMock()
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
        sess = MagicMock()
        ds_res = MagicMock(session=sess)
        ds_res.forks = MagicMock()
        ds_res.forks.index = {}

        ds = Dataset(ds_res)
        df = ds.forks_dataframe()

        assert df is None

    def test_merge_fork(self):
        fork1_url = 'http://test.crunch.io/api/datasets/abc/'
        fork2_url = 'http://test.crunch.io/api/datasets/def/'
        fork3_url = 'http://test.crunch.io/api/datasets/ghi/'
        sess = MagicMock()
        body = JSONObject({
            'name': 'ds name',
            'id': 'xyz',
            'description': 'ds description',
            'owner': 'http://test.crunch.io/api/users/123/'
        })
        ds_res = MagicMock(session=sess, body=body)
        ds_res.forks.index = {
            fork1_url: {
                'name': 'FORK #1 of ds name',
                'id': 'abc'
            },
            fork2_url: {
                'name': 'myFork',
                'id': 'def',
            },
            fork3_url: {
                'name': 'myFork',
                'id': 'ghi',
            }
        }
        ds = Dataset(ds_res)

        expected_call = {
            'dataset': fork1_url,
            'autorollback': True,
        }

        ds.merge(1)  # number as int
        ds_res.actions.post.assert_called_once_with(expected_call)
        ds_res.reset_mock()
        ds.merge('1')  # number as str
        ds_res.actions.post.assert_called_once_with(expected_call)
        ds_res.reset_mock()
        ds.merge('FORK #1 of ds name')  # name
        ds_res.actions.post.assert_called_once_with(expected_call)
        ds_res.reset_mock()
        ds.merge('abc')  # id
        ds_res.actions.post.assert_called_once_with(expected_call)
        ds_res.reset_mock()

        # test autorollback=False
        expected_call['autorollback'] = False
        ds.merge(1, autorollback=False)  # number as int
        ds_res.actions.post.assert_called_once_with(expected_call)
        ds_res.reset_mock()

        # ValueError if no unique fork could be found
        error_msg = "Couldn't find a (unique) fork. "
        "Please try again using its id"
        with pytest.raises(ValueError, message=error_msg):
            ds.merge('myFork')

        expected_call['dataset'] = fork2_url
        expected_call['autorollback'] = True

        ds.merge('def')
        ds_res.actions.post.assert_called_once_with(expected_call)
        ds_res.reset_mock()

        expected_call['dataset'] = fork3_url
        ds.merge('ghi')
        ds_res.actions.post.assert_called_once_with(expected_call)


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
        ds_mock = self._dataset_mock(variables=variables)
        ds = Dataset(ds_mock)

        responses = [
            {'id': 1, 'name': 'Facebook', 'case': 'var_a > 5'},
            {'id': 2, 'name': 'Twitter',
             'case': 'var_b < 10 and var_c in (1, 2, 3)'},
            {'id': 3, 'name': 'Google+',
             'case': '(gender == 1) and (age >= 16 and age <= 24)'},
        ]
        with pytest.raises(ValueError) as err:
            ds.create_categorical(responses, alias='cat', name='My cat', multiple=False)
        assert 'Dataset test_dataset_name has no variable' in str(err.value)
        ds.resource.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'description': '',
                'notes': '',
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
        ds_mock = self._dataset_mock(variables=variables)
        ds = Dataset(ds_mock)

        responses = [
            {'id': 1, 'name': 'Facebook', 'case': 'var_a > 5'},
            {'id': 2, 'name': 'Twitter', 'case': 'var_b < 10 and var_c in (1, 2, 3)'},
            {'id': 3, 'name': 'Google+', 'case': '(gender == 1) and (age >= 16 and age <= 24)'},
        ]
        with pytest.raises(ValueError) as err:
            ds.create_categorical(responses, alias='mr', name='my mr', multiple=True)
        assert 'Dataset test_dataset_name has no variable' in str(err.value)
        ds.resource.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'alias': 'mr',
                'description': '',
                'notes': '',
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
    ds_res = MagicMock()
    var_res = MagicMock()
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
            _var_mock = MagicMock()
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

        ds_resource = MagicMock()
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

    def test_order_iteration(self):
        ds = self.ds

        # consume all items in the dataset order
        items = [item for item in ds.order]

        assert isinstance(items[0], mock.MagicMock)  # id
        assert isinstance(items[1], mock.MagicMock)  # hobbies
        assert isinstance(items[2], scrunch.datasets.Group)  # Account
        assert isinstance(items[3], mock.MagicMock)  # music
        assert isinstance(items[4], mock.MagicMock)  # religion

    def test_order_iteration_values(self):
        ds = self.ds

        items = ds.order.values()

        assert isinstance(items[0], mock.MagicMock)  # id
        assert isinstance(items[1], mock.MagicMock)  # hobbies
        assert isinstance(items[2], scrunch.datasets.Group)  # Account
        assert isinstance(items[3], mock.MagicMock)  # music
        assert isinstance(items[4], mock.MagicMock)  # religion

    def test_order_iteration_itervalues(self):
        ds = self.ds

        items = [item for item in ds.order.itervalues()]

        assert isinstance(items[0], mock.MagicMock)  # id
        assert isinstance(items[1], mock.MagicMock)  # hobbies
        assert isinstance(items[2], scrunch.datasets.Group)  # Account
        assert isinstance(items[3], mock.MagicMock)  # music
        assert isinstance(items[4], mock.MagicMock)  # religion

    def test_order_iteration_keys(self):
        ds = self.ds

        keys = ds.order.keys()
        assert keys == ['id', 'hobbies', 'Account', 'music', 'religion']

    def test_order_iteration_iterkeys(self):
        ds = self.ds

        keys = [k for k in ds.order.iterkeys()]
        assert keys == ['id', 'hobbies', 'Account', 'music', 'religion']

    def test_order_iteration_items(self):
        ds = self.ds

        keys = []
        items = []
        for k, v in ds.order.items():
            keys.append(k)
            items.append(v)

        assert keys == ['id', 'hobbies', 'Account', 'music', 'religion']
        assert isinstance(items[0], mock.MagicMock)  # id
        assert isinstance(items[1], mock.MagicMock)  # hobbies
        assert isinstance(items[2], scrunch.datasets.Group)  # Account
        assert isinstance(items[3], mock.MagicMock)  # music
        assert isinstance(items[4], mock.MagicMock)  # religion


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

        ds_resource = MagicMock()
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
        _url = ds.resource.session.patch.call_args_list[-1][0][0]
        _payload = json.loads(ds.resource.session.patch.call_args_list[-1][0][1])
        _headers = ds.resource.session.patch.call_args_list[-1][1].get('headers', {})
        assert _url == self.ds_url + 'settings/'
        assert _payload == {'viewers_can_export': True}
        assert _headers == {'Content-Type': 'application/json'}

        ds.change_settings(
            viewers_can_export=True, viewers_can_change_weight=True,
            viewers_can_share=False
        )
        _url = ds.resource.session.patch.call_args_list[-1][0][0]
        _payload = json.loads(ds.resource.session.patch.call_args_list[-1][0][1])
        _headers = ds.resource.session.patch.call_args_list[-1][1].get('headers', {})
        assert _url == self.ds_url + 'settings/'
        assert _payload == {
            'viewers_can_export': True,
            'viewers_can_change_weight': True,
            'viewers_can_share': False
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
        _var_mock = MagicMock()
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func
        _var_mock.entity.self = var_url
        _var_mock.entity_url = var_url
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
        left_ds_res = MagicMock()
        left_ds_res.self = self.left_ds_url
        left_ds_res.variables.by.return_value = left_variable
        self.left_ds = Dataset(left_ds_res)

        # setup for right dataset
        _right_var_mock = self._variable_mock(self.right_ds_url, var)
        right_variable = collections.OrderedDict()
        right_variable[var['alias']] = _right_var_mock
        right_ds_res = MagicMock()
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


@mock.patch('scrunch.datasets.download_file')
@mock.patch('scrunch.datasets.export_dataset')
class TestDatasetExport(TestCase):

    ds_url = 'http://test.crunch.local/api/datasets/123/'
    file_download_url = 'http://test.crunch.local/download-file'

    def setUp(self):
        ds_resource = mock.MagicMock()
        ds_resource.self = self.ds_url
        self.ds = Dataset(ds_resource)

    def test_basic_csv_export(self, export_ds_mock, dl_file_mock):
        ds = self.ds
        export_ds_mock.return_value = self.file_download_url

        ds.export('export.csv')

        export_payload = export_ds_mock.call_args_list[0][0][1]
        export_format = export_ds_mock.call_args_list[0][1].get('format')
        export_options = export_payload.get('options', {})

        assert export_format == 'csv'
        assert export_options == {'use_category_ids': True}

        dl_file_mock.assert_called_with(self.file_download_url, 'export.csv')

    def test_basic_json_export(self, export_ds_mock, dl_file_mock):
        ds = self.ds
        ds.resource.table.__getitem__.return_value = 'json serializable'
        ds.export('export.csv', metadata_path='metadata.json')

        ds.resource.table.__getitem__.assert_called_with('metadata')

    def test_csv_export_options(self, export_ds_mock, dl_file_mock):
        ds = self.ds
        export_ds_mock.return_value = self.file_download_url

        ds.export('export.csv', options={'use_category_ids': False})

        export_payload = export_ds_mock.call_args_list[0][0][1]
        export_format = export_ds_mock.call_args_list[0][1].get('format')
        export_options = export_payload.get('options', {})

        assert export_format == 'csv'
        assert export_options == {'use_category_ids': False}

        dl_file_mock.assert_called_with(self.file_download_url, 'export.csv')

    def test_invalid_csv_export_options(self, export_ds_mock, _):
        ds = self.ds
        export_ds_mock.return_value = self.file_download_url

        with pytest.raises(ValueError):
            ds.export('export.csv', options={'invalid_option': False})

    def test_basic_spss_export(self, export_ds_mock, dl_file_mock):
        ds = self.ds
        export_ds_mock.return_value = self.file_download_url

        ds.export('export.sav', format='spss')

        export_payload = export_ds_mock.call_args_list[0][0][1]
        export_format = export_ds_mock.call_args_list[0][1].get('format')
        export_options = export_payload.get('options', {})

        assert export_format == 'spss'
        assert export_options == {
            'prefix_subvariables': False,
            'var_label_field': 'description'
        }

        dl_file_mock.assert_called_with(self.file_download_url, 'export.sav')

    def test_spss_export_options(self, export_ds_mock, dl_file_mock):
        ds = self.ds
        export_ds_mock.return_value = self.file_download_url

        ds.export(
            'export.sav',
            format='spss',
            options={
                'prefix_subvariables': True,
                'var_label_field': 'name'
            }
        )

        export_payload = export_ds_mock.call_args_list[0][0][1]
        export_format = export_ds_mock.call_args_list[0][1].get('format')
        export_options = export_payload.get('options', {})

        assert export_format == 'spss'
        assert export_options == {
            'prefix_subvariables': True,
            'var_label_field': 'name'}

        dl_file_mock.assert_called_with(self.file_download_url, 'export.sav')

    def test_invalid_spss_export_options(self, export_ds_mock, _):
        ds = self.ds
        export_ds_mock.return_value = self.file_download_url

        with pytest.raises(ValueError):
            ds.export(
                'export.csv', format='spss', options={'invalid_option': False}
            )

        with pytest.raises(ValueError):
            ds.export(
                'export.csv', format='spss',
                options={'var_label_field': 'invalid'}
            )


class TestVariableIterator(TestDatasetBase):

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
        }
    }

    def test_ds_keys(self):
        ds_mock = self._dataset_mock(variables=self.variables)
        ds = Dataset(ds_mock)
        assert isinstance(ds.keys(), list)

    def test_ds_values(self):
        ds_mock = self._dataset_mock(variables=self.variables)
        ds = Dataset(ds_mock)
        assert isinstance(ds.values(), list)

    def test_subvar_order(self):
        subvars_order = [
            '0001',
            '0002',
            '0003',
            '0004'
        ]
        subvars = {
            # Intentionally unordered
            '0003': {
                'id': '0003',
                'alias': 'subvar_3'
            },
            '0001': {
                'id': '0001',
                'alias': 'subvar_1'
            },
            '0004': {
                'id': '0004',
                'alias': 'subvar_4'
            },
            '0002': {
                'id': '0002',
                'alias': 'subvar_2'
            },
        }
        body = dict(subvariables=subvars_order)

        def getitem(key):
            if key == 'body':
                return body

        ds = mock.MagicMock()
        var_tuple = mock.MagicMock()
        var_tuple.entity.__getitem__.side_effect = getitem
        var_tuple.entity.subvariables.index = subvars

        v = Variable(var_tuple=var_tuple, dataset=ds)

        all_ids = [sv['id'] for sv in v]
        assert all_ids == ['0001', '0002', '0003', '0004']


class TestFilter(TestDatasetBase, TestCase):

    _filter = {
        "element": "shoji:entity",
        "self": "https://alpha.crunch.io/api/datasets/1/filters/1/",
        "description": "Detail information for one filter",
        "body": {
            "id": "326d5db5a40f4189a8a4cddfe06bb19c",
            "name": "easy",
            "is_public": True,
            "expression": {
                "function": "in",
                    "args": [
                        {
                            "variable": "https://alpha.crunch.io/api/datasets/1/variables/1/"
                        },
                        {
                            "value": 1
                        }
                    ],
            }
        }
    }

    @mock.patch('scrunch.datasets.Dataset.filters')
    def test_add_filter(self, filters):
        ds_res = self._dataset_mock()
        ds = Dataset(ds_res)
        var = ds['var1_alias']

        ds.add_filter(name='filter', expr='var1_alias != 0')

        expected_payload = {
            'element': 'shoji:entity',
            'body': {
                'name': 'filter',
                'is_public': False,
                'expression': {
                    'function': '!=',
                    'args': [
                        {'variable': var.url},
                        {'value': 0}
                    ]
                }
            }
        }
        ds.resource.filters.create.assert_called_with(expected_payload)

    def test_edit_filter(self):
        filter = EditableMock(entity=self._filter)
        mockfilter = Filter(filter)
        with pytest.raises(AttributeError):
            mockfilter.edit(name='edited')
            mockfilter.resource.edit.assert_called_with({'name': 'edited'})

    def test_filter_class(self):
        filter = MagicMock(entity=self._filter)
        mockfilter = Filter(filter)
        assert mockfilter
