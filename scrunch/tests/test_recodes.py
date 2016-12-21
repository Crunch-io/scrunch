import mock
from unittest import TestCase

import pytest
from scrunch.datasets import Dataset, Variable
from scrunch.datasets import validate_category_map, responses_from_map

from pycrunch.shoji import Entity


dataset_url = 'http://test.crunch.io/api/datasets/123/'
var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
subvar1_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00001/'
subvar2_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00002/'


CATEGORY_MAP = [
    {'id': 1, 'name': 'China', 'missing': False, 'combined_ids': [2, 3]},
    {'id': 2, 'name': 'Other', 'missing': False, 'combined_ids': [1]}
]

RESPONSE_MAP = [
    {"id": 1, "name": 'online', 'combined_ids': [1, 2]},
]

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


class TestRecodes(TestCase):

    def test_validate_range_expression(self):
        test_map = [{
            "id": 1,
            "name": "China",
            "missing": False,
            "combined_ids": range(1, 5)
        }]
        variable_mock = mock.MagicMock()
        subvar_mock = mock.MagicMock(entity_url=subvar1_url)
        variable_mock.subvariables.by.return_value = {
            'parent_1': subvar_mock,
            'parent_2': subvar_mock,
            'parent_3': subvar_mock,
            'parent_4': subvar_mock,
        }
        parent_var = Variable(variable_mock)
        modified_map = responses_from_map(parent_var, test_map, 'test', 'parent')
        # subvar_url * 4 because we used the same mock for all subvars
        assert modified_map[0]['combined_ids'] == [subvar1_url] * 4

    def test_combine_categories_unknown_alias(self):
        resource = mock.MagicMock()
        entity_mock = mock.MagicMock()
        entity_mock.entity_url = var_url
        resource.variables.by.return_value = {
            'test': entity_mock
        }
        ds = Dataset(resource)
        with pytest.raises(ValueError) as err:
            ds.combine_categories('unknown', CATEGORY_MAP, 'name', 'alias')

        assert 'Dataset has no variable unknown' in str(err.value)

    def test_combine_categories_from_alias(self):
        resource = mock.MagicMock()
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url
        resource.variables.by.return_value = {
            'test': entity_mock
        }
        ds = Dataset(resource)
        ds.combine_categories('test', CATEGORY_MAP, 'name', 'alias')
        ds.resource.variables.create.assert_called_with(RECODES_PAYLOAD)

    def test_combine_categories_from_entity(self):
        resource = mock.MagicMock()
        entity_mock = mock.MagicMock()
        resource.variables.by.return_value = {
            'test': entity_mock
        }
        entity = Variable(Entity(mock.MagicMock(), self=var_url, body={}))
        ds = Dataset(resource)
        ds.combine_categories(entity, CATEGORY_MAP, 'name', 'alias')
        ds.resource.variables.create.assert_called_with(RECODES_PAYLOAD)

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

        ds = Dataset(resource)
        with pytest.raises(ValueError) as err:
            ds.combine_responses('test', RESPONSE_MAP, name='name', alias='alias')

        assert 'Unknown subvariables for variable' in str(err.value)

    def test_combine_responses_by_alias(self):
        resource = mock.MagicMock()
        resource.entity.self = dataset_url

        # mock subvariables
        subvar_mock = mock.MagicMock()
        subvar_mock.entity_url = subvar1_url
        subvar2_mock = mock.MagicMock()
        subvar2_mock.entity_url = subvar2_url

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
        ds = Dataset(resource)
        ds.combine_responses('test', RESPONSE_MAP, name='name', alias='alias')
        resource.variables.create.assert_called_with(COMBINE_RESPONSES_PAYLOAD)

    def test_combine_responses_by_entity(self):
        ds_resource = mock.MagicMock()
        ds_resource.entity.self = dataset_url

        # mock subvariables
        subvar_mock = mock.MagicMock(entity_url=subvar1_url)
        subvar2_mock = mock.MagicMock(entity_url=subvar2_url)

        # mock parent variable
        parent_variable = mock.MagicMock(body={'alias': 'test'})
        parent_variable.self = var_url

        # add dictionaries return to by functions
        parent_variable.subvariables.by.return_value = {
            'test_1': subvar_mock,
            'test_2': subvar2_mock
        }

        ds_resource.variables.by.return_value = {
            'test': parent_variable
        }

        entity = Variable(parent_variable)

        # make the actual response call
        ds = Dataset(ds_resource)
        ds.combine_responses(entity, RESPONSE_MAP, 'name', 'alias')
        ds_resource.variables.create.assert_called_with(COMBINE_RESPONSES_PAYLOAD)


class TestSpssRecodes(TestCase):

    @mock.patch('scrunch.datasets.get_dataset')
    def test_recode_categoricals(self, get_dataset_mock):
        dataset_id = '123'
        var_res = Entity(mock.MagicMock(), **{
            'element': 'shoji:entity',
            'self': ('http://test.crunch.io/api/datasets/%s/variables/0001/'
                     % dataset_id),
            'body': {
                'description': ('Which of the following best describes your '
                                'sexuality?'),
                'categories': [
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
                ],
                'type': 'categorical',
                'alias': 'sexuality',
                'name': 'Sexuality',
                'dataset_id': dataset_id
            }
        })
        var = Variable(var_res)

        var.combine(
            'sexuality2', {1: 1, 2: (2, 3, 4, 5)}, ('Straight', 'LGBT')
        )

        # Test how the recoded var was created.
        var_create_mock = get_dataset_mock(dataset_id).variables.create
        assert len(var_create_mock.call_args_list) == 1

        call = var_create_mock.call_args_list[0]
        payload = call[0][0]

        assert payload == {
            'element': 'shoji:entity',
            'body': {
                'alias': 'sexuality2',
                'description': 'Which of the following best describes your sexuality?',
                'expr': {
                    'function': 'combine_categories',
                    'args': [
                        {
                            'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/'
                        },
                        {
                            'value': [
                                {
                                    'missing': False,
                                    'combined_ids': [
                                        1
                                    ],
                                    'id': 1,
                                    'name': 'Straight'
                                },
                                {
                                    'missing': False,
                                    'combined_ids': [
                                        2,
                                        3,
                                        4,
                                        5
                                    ],
                                    'id': 2,
                                    'name': 'LGBT'
                                },
                                {
                                    'missing': True,
                                    'combined_ids': [
                                        8,
                                        9
                                    ],
                                    'id': 3,
                                    'name': 'Missing'
                                }
                            ]
                        }
                    ]
                },
                'name': 'Sexuality (recoded)'
            }
        }

    @mock.patch('scrunch.datasets.get_dataset')
    def test_recode_multiple_responses(self, get_dataset_mock):
        dataset_id = '123'
        var_res = Entity(mock.MagicMock(), **{
            'element': 'shoji:entity',
            'self': 'http://test.crunch.io/api/datasets/%s/variables/0001/' % dataset_id,
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
                'categories': [
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
                ],
                'description': 'Multiple Response Example'
            }

        })
        var = Variable(var_res)

        var.combine(
            'Q1_recoded',
            {'Q1_recoded_1': ('Q1_1', 'Q1_2'), 'Q1_recoded_2': 'Q1_3'}
        )

        # Test how the recoded var was created.
        var_create_mock = get_dataset_mock(dataset_id).variables.create
        assert len(var_create_mock.call_args_list) == 1

        call = var_create_mock.call_args_list[0]
        payload = call[0][0]

        assert payload == {
            'body': {
                'alias': 'Q1_recoded',
                'expr': {
                    'function': 'combine_responses',
                    'args': [
                        {
                            'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/'
                        },
                        {
                            'value': [
                                {
                                    'name': 'Q1_recoded_1',
                                    'combined_ids': [
                                        'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/000a/',
                                        'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/000b/'
                                    ]
                                },
                                {
                                    'name': 'Q1_recoded_2',
                                    'combined_ids': [
                                        'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/000c/'
                                    ]
                                }
                            ]
                        }
                    ]
                },
                'description': 'Multiple Response Example',
                'name': 'Q1 (recoded)'
            },
            'element': 'shoji:entity'
        }
