import mock
from unittest import TestCase

import pytest
from scrunch.datasets import Variable
from scrunch.mutable_dataset import MutableDataset
from scrunch.variables import responses_from_map
from scrunch.helpers import subvar_alias

from pycrunch.shoji import Entity


def _any(self, column):
    return {
        'function': 'any',
        'args': [{
            'variable': self
        }, {
            'column': column
        }]
    }


def mr_in(mr_self, mr_alias, groups, parent_subvariables):
    """
    Similar helper as in examples.py. Has to return an `any` function.
    """
    return _any(mr_self, [parent_subvariables[subvar_alias(mr_alias, sv)].id
                          for sv in groups])


dataset_url = 'http://test.crunch.io/api/datasets/123/'
var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
subvar1_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00001/'
subvar2_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00002/'


CATEGORY_MAP = {
    1: [2, 3],
    2: 1
}
CATEGORY_NAMES = {
    1: 'China',
    2: 'Other'
}

RESPONSE_MAP = {
    1: [1, 2]
}
RESPONSE_NAMES = {
    1: 'online'
}

RECODES_PAYLOAD = {
    'element': 'shoji:entity',
    'body': {
        'alias': 'alias',
        'derivation': {
            'function': 'combine_categories',
            'args': [
                {'variable': var_url},
                {'value': [
                    {'combined_ids': [2, 3], 'missing': False, 'id': 1, 'name': 'China'},
                    {'combined_ids': [1], 'missing': False, 'id': 2, 'name': 'Other'}
                ]}
            ]
        },
        'name': 'name',
        'description': ''
    }
}

COMBINE_RESPONSES_PAYLOAD = {
    'element': 'shoji:entity',
    'body': {
        'name': 'name',
        'description': '',
        'alias': 'alias',
        'derivation': {
            'function': 'combine_responses',
            'args': [
                {'variable': var_url},
                {'value': [
                    {'alias': 'alias_1', 'combined_ids': [subvar1_url, subvar2_url], 'name': 'online'}
                ]}
            ]
        }
    }
}


class TestCombine(TestCase):

    def test_validate_range_expression(self):
        test_map = {
            1: range(1, 5)
        }
        test_cats = {
            1: "China"
        }
        ds_res_mock = mock.MagicMock()
        variable_mock = mock.MagicMock()
        subvar_mock = mock.MagicMock(entity_url=subvar1_url)
        # mock the call to entity, this will happen on Variable.resource
        variable_mock.entity.subvariables.by.return_value = {
            'parent_1': subvar_mock,
            'parent_2': subvar_mock,
            'parent_3': subvar_mock,
            'parent_4': subvar_mock,
        }
        parent_var = Variable(variable_mock, ds_res_mock)
        modified_map = responses_from_map(parent_var, test_map, test_cats, 'test', 'parent')
        # subvar_url * 4 because we used the same mock for all subvars
        assert modified_map[0]['combined_ids'] == [subvar1_url] * 4

    def test_validate_integer(self):
        test_map = {
            1: 1
        }
        test_cats = {
            1: "China"
        }

        ds_res_mock = mock.MagicMock()
        variable_mock = mock.MagicMock()
        subvar_mock = mock.MagicMock(entity_url=subvar1_url)
        # mock the call to entity, this will happen on Variable.resource
        variable_mock.entity.subvariables.by.return_value = {
            'parent_1': subvar_mock
        }
        parent_var = Variable(variable_mock, ds_res_mock)
        modified_map = responses_from_map(parent_var, test_map, test_cats, 'test', 'parent')
        # subvar_url * 4 because we used the same mock for all subvars
        assert modified_map[0]['combined_ids'] == [subvar1_url]

    def test_combine_categories_unknown_alias(self):
        resource = mock.MagicMock()
        resource.body = {'name': 'mocked_dataset'}
        entity_mock = mock.MagicMock(entity_url=var_url)
        resource.variables.by.return_value = {
            'test': entity_mock
        }
        resource.variables.index = {}  # Var not present
        ds = MutableDataset(resource)
        with pytest.raises(ValueError) as err:
            ds.combine_categorical('unknown', CATEGORY_MAP, CATEGORY_NAMES, name='name', alias='alias')

        assert 'Entity mocked_dataset has no (sub)variable with a name or alias unknown' in str(err.value)

    def test_combine_categories_from_alias(self):
        resource = mock.MagicMock()
        resource.body = {'name': 'mocked_dataset'}
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url
        resource.variables.by.return_value = {
            'test': entity_mock,
        }
        resource.variables.index = {}
        ds = MutableDataset(resource)
        with pytest.raises(ValueError) as err:
            ds.combine_categorical('test', CATEGORY_MAP, CATEGORY_NAMES, name='name', alias='alias')
        ds.resource.variables.create.assert_called_with(RECODES_PAYLOAD)
        assert 'Entity mocked_dataset has no (sub)variable' in str(err.value)

    def test_combine_categories_from_entity(self):
        resource = mock.MagicMock()
        resource.body = {'name': 'mocked_dataset'}
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url
        resource.variables.by.return_value = {
            'test': entity_mock
        }
        resource.variables.index = {}  # Var not present

        # mock a Tuple object
        tuple_mock = mock.MagicMock()
        tuple_mock.entity.self = var_url

        entity = Variable(tuple_mock, resource)
        ds = MutableDataset(resource)
        with pytest.raises(ValueError) as err:
            ds.combine_categorical(entity, CATEGORY_MAP, CATEGORY_NAMES, name='name', alias='alias')
        ds.resource.variables.create.assert_called_with(RECODES_PAYLOAD)
        assert 'Entity mocked_dataset has no (sub)variable' in str(err.value)

    def test_combine_responses_unknown_alias(self):
        resource = mock.MagicMock()
        resource.entity.self = dataset_url

        # mock subvariables
        subvar_mock = mock.MagicMock()
        subvar_mock.entity.self = subvar1_url
        subvar2_mock = mock.MagicMock()
        subvar2_mock.entity.self = subvar2_url

        # mock parent variable
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url

        # add dictionaries return to by functions
        entity_mock.entity.subvariables.by.return_value = {
            'test_1': subvar_mock,
            'test_x': subvar2_mock
        }

        resource.variables.by.return_value = {
            'test': entity_mock
        }

        ds = MutableDataset(resource)
        with pytest.raises(ValueError) as err:
            ds.combine_multiple_response('test', RESPONSE_MAP, RESPONSE_NAMES, name='name', alias='alias')

        assert 'Unknown subvariables for variable' in str(err.value)

    def test_combine_responses_by_alias(self):
        resource = mock.MagicMock()
        resource.body = {'name': 'mocked_dataset'}
        resource.entity.self = dataset_url
        resource.variables.index = {}  # Var not present
        # mock subvariables
        subvar_mock = mock.MagicMock(entity_url=subvar1_url)
        subvar2_mock = mock.MagicMock(entity_url=subvar2_url)

        # mock parent variable
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url

        # add dictionaries return to by functions
        entity_mock.entity.subvariables.by.return_value = {
            'test_1': subvar_mock,
            'test_2': subvar2_mock
        }

        resource.variables.by.return_value = {
            'test': entity_mock
        }

        # make the actual response call
        ds = MutableDataset(resource)
        with pytest.raises(ValueError) as err:
            ds.combine_multiple_response('test', RESPONSE_MAP, RESPONSE_NAMES, name='name', alias='alias')
        resource.variables.create.assert_called_with(COMBINE_RESPONSES_PAYLOAD)
        assert 'Entity mocked_dataset has no (sub)variable' in str(err.value)

    def test_combine_responses_by_entity(self):
        resource = mock.MagicMock()
        resource.body = {'name': 'mocked_dataset'}
        resource.entity.self = dataset_url
        resource.variables.index = {}  # Var not present

        # mock subvariables
        subvar_mock = mock.MagicMock(entity_url=subvar1_url)
        subvar_mock.entity.self = subvar1_url
        subvar2_mock = mock.MagicMock(entity_url=subvar2_url)

        # mock parent variable
        entity_mock = mock.MagicMock()
        # need to name the var to actually build subvar names
        entity_mock.alias = 'test'
        entity_mock.resource.self = var_url

        # add dictionaries return to by functions
        entity_mock.resource.subvariables.by.return_value = {
            'test_1': subvar_mock,
            'test_2': subvar2_mock
        }

        resource.variables.by.return_value = {
            'test': entity_mock
        }

        ds = MutableDataset(resource)

        with pytest.raises(ValueError) as err:
            ds.combine_multiple_response(entity_mock, RESPONSE_MAP, RESPONSE_NAMES, name='name', alias='alias')
        resource.variables.create.assert_called_with(COMBINE_RESPONSES_PAYLOAD)
        assert 'Entity mocked_dataset has no (sub)variable' in str(err.value)


class TestRecode(TestCase):

    @mock.patch('scrunch.mutable_dataset.get_mutable_dataset')
    def test_recode_categoricals(self, get_dataset_mock):
        categories = [
            {
                'missing': False,
                'name': 'Heterosexual',
                'numeric_value': 1,
                'id': 1
            },
            {
                'missing': False,
                'name': 'Gay or lesbian',
                'numeric_value': 2,
                'id': 2
            },
            {
                'missing': False,
                'name': 'Bisexual',
                'numeric_value': 3,
                'id': 3
            },
            {
                'missing': False,
                'name': 'Other',
                'numeric_value': 4,
                'id': 4
            },
            {
                'missing': False,
                'name': 'Prefer not to say',
                'numeric_value': 5,
                'id': 5
            },
            {
                'missing': True,
                'name': 'skipped',
                'numeric_value': None,
                'id': 8
            },
            {
                'missing': True,
                'name': 'not asked',
                'numeric_value': None,
                'id': 9
            }
        ]
        table_mock = mock.MagicMock(metadata={
            '00001': {
                'id': '00001',
                'alias': 'sexuality',
                'type': 'categorical',
                'categories': categories
            }
        })

        ds_res = mock.MagicMock()
        ds_res.self = dataset_url
        ds_res.follow.return_value = table_mock
        dataset = MutableDataset(ds_res)
        dataset.create_categorical([
            {'id': 1, 'name': 'Straight', 'case': 'sexuality.any([1])'},
            {'id': 2, 'name': 'LGBTQ+', 'case': 'sexuality.any([2, 3, 4, 5])'}
        ], name='Sexuality 2', alias='sexuality2', multiple=False)

        ds_res.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'name': 'Sexuality 2',
                'alias': 'sexuality2',
                'description': '',
                'notes': '',
                'expr': {
                    'function': 'case',
                    'args': [{
                        'column': [1, 2, -1],
                        'type': {
                            'value': {
                                'class': 'categorical',
                                'categories': [
                                    {'missing': False, 'id': 1, 'name': 'Straight', 'numeric_value': None},
                                    {'missing': False, 'id': 2, 'name': 'LGBTQ+', 'numeric_value': None},
                                    {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
                                ]
                            }
                        }
                    }, {
                        'function': 'any',
                        'args': [
                            {'variable': 'http://test.crunch.io/api/datasets/123/variables/00001/'},
                            {'value': [1]}
                        ]
                    }, {
                        'function': 'any',
                        'args': [
                            {'variable': 'http://test.crunch.io/api/datasets/123/variables/00001/'},
                            {'value': [2, 3, 4, 5]}
                        ]
                    }]
                }
            }
        })

    @mock.patch('scrunch.mutable_dataset.get_mutable_dataset')
    def test_recode_multiple_responses(self, get_dataset_mock):
        dataset_id = '123'
        categories = [
            {
                'numeric_value': 1,
                'selected': True,
                'id': 1,
                'name': 'selected',
                'missing': False
            },
            {
                'numeric_value': 2,
                'selected': False,
                'id': 2,
                'name': 'not selected',
                'missing': False
            },
            {
                'numeric_value': 9,
                'missing': True,
                'id': 9,
                'name': 'not asked'
            },
            {
                'numeric_value': 8,
                'missing': True,
                'id': 8,
                'name': 'skipped'
            }
        ]
        Entity(mock.MagicMock(), **{
            'element': 'shoji:entity',
            'self': 'http://test.crunch.io/api/datasets/%s/variables/0001/' % dataset_id,
            # needed in order to simulate a Tuple, now Variable is inited with Tuple
            'entity_url': 'http://test.crunch.io/api/datasets/%s/variables/0001/' % dataset_id,
            'body': {
                'name': 'Q1',
                'subreferences': [
                    {
                        'alias': 'Q1_1',
                        'is_subvar': True,
                        'name': 'One'
                    },
                    {
                        'alias': 'Q1_2',
                        'is_subvar': True,
                        'name': 'Two'
                    },
                    {
                        'alias': 'Q1_3',
                        'is_subvar': True,
                        'name': 'Three'
                    }
                ],
                'missing_reasons': {
                    'skipped': 8,
                    'not asked': 9
                },
                'alias': 'Q1',
                'subvariables': [
                    'http://test.crunch.io/api/datasets/%s/variables/0001/subvariables/000a/' % dataset_id,
                    'http://test.crunch.io/api/datasets/%s/variables/0001/subvariables/000b/' % dataset_id,
                    'http://test.crunch.io/api/datasets/%s/variables/0001/subvariables/000c/' % dataset_id
                ],
                'dataset_id': dataset_id,
                'type': 'multiple_response',
                'id': '0001',
                'categories': categories,
                'description': 'Multiple Response Example',
                'notes': '',
            }

        })
        table_mock = mock.MagicMock(metadata={
            '00001': {
                'id': '00001',
                'alias': 'sexuality',
                'type': 'categorical',
                'notes': '',
                'categories': categories
            }
        })
        ds_res = mock.MagicMock()
        ds_res.self = dataset_url
        ds_res.follow.return_value = table_mock
        dataset = MutableDataset(ds_res)
        subvar_mock = mock.MagicMock()
        subvar_mock.self = var_url
        subvar_mock.id = 'subvar'
        subvariables = {
            'Q1_1': subvar_mock,
            'Q1_2': subvar_mock,
            'Q1_3': subvar_mock,
        }

        dataset.create_categorical([
            {'id': 1, 'name': 'Q1_recoded_1', 'case': mr_in(var_url, 'Q1', [1, 2], subvariables)},
            {'id': 2, 'name': 'Q1_recoded_2', 'case': mr_in(var_url, 'Q1', [3], subvariables)}
        ], alias='Q1_recoded', name='Q1_recoded', multiple=True)

        # Test how the recoded var was created.
        ds_res.variables.create.assert_called_with({
            'element': 'shoji:entity',
            'body': {
                'name': 'Q1_recoded',
                'description': '',
                'notes': '',
                'alias': 'Q1_recoded',
                'derivation': {
                    'function': 'array',
                    'args': [{
                        'function': 'select',
                        'args': [{
                            'map': {
                                '0001': {
                                    'function': 'case',
                                    'args': [{
                                        'column': [1, 2],
                                        'type': {
                                            'value': {
                                                'class': 'categorical',
                                                'categories': [
                                                    {'selected': True, 'numeric_value': None, 'missing': False, 'id': 1, 'name': 'Selected'},
                                                    {'selected': False, 'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Not selected'}
                                                ]
                                            }
                                        }
                                    }, {
                                        'function': 'any',
                                        'args': [
                                            {'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/'},
                                            {'column': ['subvar', 'subvar']}
                                        ]
                                    }],
                                    'references': {
                                        'alias': 'Q1_recoded_1',
                                        'name': 'Q1_recoded_1'
                                    }
                                },
                                '0002': {
                                    'function': 'case',
                                    'args': [{
                                        'column': [1, 2],
                                        'type': {
                                            'value': {
                                                'class': 'categorical',
                                                'categories': [
                                                    {'selected': True, 'numeric_value': None, 'missing': False, 'id': 1, 'name': 'Selected'},
                                                    {'selected': False, 'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Not selected'}
                                                ]
                                            }
                                        }
                                    }, {
                                        'function': 'any',
                                        'args': [
                                            {'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/'},
                                            {'column': ['subvar']}
                                        ]
                                    }],
                                    'references': {
                                        'alias': 'Q1_recoded_2',
                                        'name': 'Q1_recoded_2'
                                    }
                                }
                            }
                        }, {
                            'value': [
                                '0001',
                                '0002'
                            ]
                        }]
                    }]
                }
            }
        })
