import mock
from unittest import TestCase

import pytest
from scrunch.datasets import Dataset, Variable
from scrunch.datasets import validate_category_map

from pycrunch.shoji import Entity


CATEGORY_MAP = {
    1: {
        "name": "China",
        "missing": False,
        "combined_ids": [2, 3]
    },
    2: {
        "name": "Other",
        "missing": False,
        "combined_ids": [1]
    }
}

RESPONSE_MAP = {
    'newsubvar': ['sub1', 'sub2']
}

RECODES_PAYLOAD = {
    "element": "shoji:entity",
    "body": {
        "name": "name",
        "description": "",
        "alias": "alias",
        "expr": {
            "function": "combine_categories",
            "args": [
                {
                    "variable": 'http://test.crunch.io/api/datasets/123/variables/0001/'
                },
                {
                    "value": [
                        {
                            "name": "China",
                            "id": 1,
                            "missing": False,
                            "combined_ids": [2, 3]
                        },
                        {
                            "name": "Other",
                            "id": 2,
                            "missing": False,
                            "combined_ids": [1]
                        }
                    ]
                }
            ]
        }
    }
}

COMBINE_RESPONSES_PAYLOAD = {
    'element': 'shoji:entity',
    'body': {
        'alias': 'alias',
        'description': '',
        'name': 'name',
        'expr': {
            'function': 'combine_responses',
            'args': [
                {
                    'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/'
                },
                {
                    'value': [
                        {
                            'name': 'newsubvar',
                            'combined_ids': [
                                'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00001/',
                                'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00002/'
                            ]
                        }
                    ]
                }
            ]
        }
    }
}


class TestRecodes(TestCase):

    def test_validate_category_map(self):
        """ Validate we are properly converting the given map of categories """
        expected_map = [
            {
                "id": 1,
                "name": "China",
                "missing": False,
                "combined_ids": [2, 3]
            },
            {
                "id": 2,
                "name": "Other",
                "missing": False,
                "combined_ids": [1]
            }
        ]
        modified_map = validate_category_map(CATEGORY_MAP)
        assert modified_map == expected_map

    def test_validate_range_expression(self):
        test_map = {
            1: {
                "name": "China",
                "missing": False,
                "combined_ids": range(1, 5)
            }
        }
        modified_map = validate_category_map(test_map)
        assert modified_map[0]['combined_ids'] == [1, 2, 3, 4]

    def test_combine_categories_unknown_alias(self):
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_categories = Dataset.combine_categories
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url
        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }
        with pytest.raises(KeyError) as err:
            ds.combine_categories(ds, 'unknown', CATEGORY_MAP, 'name', 'alias')

        assert 'Variable unknown does not exist in Dataset' in str(err.value)

    def test_combine_categories_from_alias(self):
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_categories = Dataset.combine_categories
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url
        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }
        ds.combine_categories(ds, 'test', CATEGORY_MAP, 'name', 'alias')
        call = ds.resource.variables.create.call_args_list[0][0][0]

        assert call == RECODES_PAYLOAD

    def test_combine_categories_from_url(self):
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_categories = Dataset.combine_categories
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url
        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }
        ds.combine_categories(ds, var_url, CATEGORY_MAP, 'name', 'alias')
        call = ds.resource.variables.create.call_args_list[0][0][0]

        assert call == RECODES_PAYLOAD

    def test_combine_categories_from_entity(self):
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_categories = Dataset.combine_categories
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'
        entity_mock = mock.MagicMock()
        entity_mock.entity.self = var_url
        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }
        entity = Entity(mock.MagicMock(), self=var_url, body={})
        ds.combine_categories(ds, entity, CATEGORY_MAP, 'name', 'alias')
        call = ds.resource.variables.create.call_args_list[0][0][0]

        assert call == RECODES_PAYLOAD

    def test_combine_responses_unknown_alias(self):
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_responses = Dataset.combine_responses
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        subvar1_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00001/'
        subvar2_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00002/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'

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
            'sub1': subvar_mock,
            'sub2': subvar2_mock
        }

        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }

        # mock response from ds.session.get(variable_url)
        var_response = mock.MagicMock()
        var_response.payload = entity_mock.entity
        ds.resource.session.get.return_value = var_response
        response_map = {
            'newsubvar': ['unknown', 'sub1', 'sub2']
        }
        with pytest.raises(KeyError) as err:
            ds.combine_responses(ds, 'test', response_map, 'name', 'alias')

        assert 'Unexistant variables' in str(err.value)

    def test_combine_responses_by_alias(self):
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_responses = Dataset.combine_responses
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        subvar1_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00001/'
        subvar2_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00002/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'

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
            'sub1': subvar_mock,
            'sub2': subvar2_mock
        }

        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }

        # mock response from ds.session.get(variable_url)
        var_response = mock.MagicMock()
        var_response.payload = entity_mock.entity
        ds.resource.session.get.return_value = var_response

        # make the actual response call
        ds.combine_responses(ds, 'test', RESPONSE_MAP, 'name', 'alias')
        call = ds.resource.variables.create.call_args_list[0][0][0]

        assert call == COMBINE_RESPONSES_PAYLOAD

    def test_combine_responses_by_url(self):
        assert False, "Should not support by URL"
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_responses = Dataset.combine_responses
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        subvar1_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00001/'
        subvar2_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00002/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'

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
            'sub1': subvar_mock,
            'sub2': subvar2_mock
        }

        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }

        # mock response from ds.session.get(variable_url)
        var_response = mock.MagicMock()
        var_response.payload = entity_mock.entity
        ds.resource.session.get.return_value = var_response

        # make the actual response call
        ds.combine_responses(ds, 'test', RESPONSE_MAP, 'name', 'alias')
        call = ds.resource.variables.create.call_args_list[0][0][0]

        assert call == COMBINE_RESPONSES_PAYLOAD

    def test_combine_responses_by_entity(self):
        ds = mock.MagicMock()
        ds.__class__ = Dataset
        ds.combine_responses = Dataset.combine_responses
        var_url = 'http://test.crunch.io/api/datasets/123/variables/0001/'
        subvar1_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00001/'
        subvar2_url = 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/00002/'
        ds.resource.entity.self = 'http://test.crunch.io/api/datasets/123/'

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
            'sub1': subvar_mock,
            'sub2': subvar2_mock
        }

        ds.resource.variables.by.return_value = {
            'test': entity_mock
        }

        # mock response from ds.session.get(variable_url)
        var_response = mock.MagicMock()
        var_response.payload = entity_mock.entity
        ds.resource.session.get.return_value = var_response

        entity = Entity(
            mock.MagicMock(),
            self=var_url,
            body={}
        )

        # make the actual response call
        ds.combine_responses(ds, entity, RESPONSE_MAP, 'name', 'alias')
        call = ds.resource.variables.create.call_args_list[0][0][0]

        assert call == COMBINE_RESPONSES_PAYLOAD


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
