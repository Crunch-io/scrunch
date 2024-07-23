# coding: utf-8
import os


import pytest
from pycrunch.importing import Importer
from pycrunch.shoji import as_entity

from fixtures import BaseIntegrationTestCase, MR_CATS
from scrunch.mutable_dataset import get_mutable_dataset


@pytest.mark.skipif(os.environ.get("LOCAL_INTEGRATION") is not None, reason="Do not run this test during CI/CD")
class TestExpressions(BaseIntegrationTestCase):

    def _create_mr_dataset(self, name, rows):
        _dataset_metadata = {
            "mr_variable": {
                "name": "Multiple Response",
                "type": "multiple_response",
                "categories": MR_CATS,
                "subreferences": [{
                    "name": "Response 1",
                    "alias": "response_1"
                }, {
                    "name": "Response 2",
                    "alias": "response_2"
                }, {
                    "name": "Response 3",
                    "alias": "response_3"
                }]
            },
        }
        ds = self.site.datasets.create({
            'element': 'shoji:entity',
            'body': {
                'name': name,
                'table': {
                    'element': 'crunch:table',
                    'metadata': _dataset_metadata
                },
            }
        }).refresh()
        Importer().append_rows(ds, rows)
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        return ds, scrunch_dataset

    def test_multiple_response_all_add_filter_value(self):
        ds_rows = [
            ["response_1", "response_2", "response_3"],
            [2, 2, 1],
            [1, 2, 2],
            [1, 1, 1]
        ]
        ds, scrunch_dataset = self._create_mr_dataset('test_mr_any', ds_rows)
        _filter = "mr_variable.all([1])"
        try:
            resp = scrunch_dataset.add_filter(name='filter_1', expr=_filter)
            data = ds.follow("table", "limit=20&filter={}".format(resp.resource.self))['data']
            ds_variables = ds.variables.by("alias")
            mr_variable_id = ds_variables["mr_variable"].id
            assert data[mr_variable_id] == [
                [1, 1, 1]
            ]
        finally:
            # cleanup
            ds.delete()

    def test_multiple_response_any_add_filter_single_subvar(self):
        ds_rows = [
            ["response_1", "response_2", "response_3"],
            [2, 2, 1],
            [1, 2, 2],
            [2, 1, 1]
        ]
        ds, scrunch_dataset = self._create_mr_dataset('test_mr_any', ds_rows)
        _filter = "mr_variable.any([response_1])"
        try:
            resp = scrunch_dataset.add_filter(name='filter_1', expr=_filter)
            data = ds.follow("table", "limit=20&filter={}".format(resp.resource.self))['data']
            ds_variables = ds.variables.by("alias")
            mr_variable_id = ds_variables["mr_variable"].id
            assert data[mr_variable_id] == [
                [1, 2, 2]
            ]
        finally:
            # cleanup
            ds.delete()

    def test_multiple_response_any_add_filter_subvar(self):
        ds_rows = [
            ["response_1", "response_2", "response_3"],
            [2, 2, 1],
            [2, 2, 2],
            [2, 1, 1]
        ]
        ds, scrunch_dataset = self._create_mr_dataset('test_mr_any', ds_rows)
        _filter = "mr_variable.any([response_1, response_2])"
        try:
            resp = scrunch_dataset.add_filter(name='filter_1', expr=_filter)
            data = ds.follow("table", "limit=20&filter={}".format(resp.resource.self))['data']
            ds_variables = ds.variables.by("alias")
            mr_variable_id = ds_variables["mr_variable"].id
            assert data[mr_variable_id] == [
                [2, 1, 1]
             ]
        finally:
            # cleanup
            ds.delete()

    def test_categorical_array_any_add_filter(self):
        ds = self.site.datasets.create(as_entity({"name": "test_any_categorical_add_filter"})).refresh()
        ds.variables.create(as_entity({
            "name": "Categorical Var",
            "alias": "categorical_var",
            "type": "categorical_array",
            "categories": [
                {
                    'id': 1,
                    'missing': False,
                    'name': 'Very interested',
                    'numeric_value': 1
                },
                {
                    'id': 2,
                    'missing': False,
                    'name': 'Somewhat interested',
                    'numeric_value': 2
                },
                {
                    'id': 3,
                    'missing': False,
                    'name': 'A little interested',
                    'numeric_value': 3
                },
                {
                    'id': -1,
                    'missing': True,
                    'name': 'No Data',
                    'numeric_value': None
                }
            ],
            'subvariables': [
                {
                    'alias': 'response_1',
                    'name': 'Response1'
                },
                {
                    'alias': 'response_2',
                    'name': 'Response2'
                },
                {
                    'alias': 'response_3',
                    'name': 'Response3'
                },
            ],
            "values": [
                [1, 3, -1],
                [2, 1, 1],
                [2, 3, 2]
            ]
        }))
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        _filter = "categorical_var.any([1])"
        try:
            resp = scrunch_dataset.add_filter(name='filter_1', expr=_filter)
            data = ds.follow("table", "limit=20&filter={}".format(resp.resource.self))['data']
            ds_variables = ds.variables.by("alias")
            cat_var_variable_id = ds_variables["categorical_var"].id
            assert data[cat_var_variable_id] == [
                [1, 3, {"?": -1}],
                [2, 1, 1]
            ]
            assert resp.resource.body.entity.body.definition == {
                "function": "or",
                "args": [
                    {
                        "function": "in",
                        "args": [
                            {
                                "var": "categorical_var",
                                "axes": ["response_1"]
                            },
                            {
                                "value": [1]
                            }
                        ]
                    },
                    {
                        "function": "in",
                        "args": [{
                            "var": "categorical_var",
                            "axes": ["response_2"]
                        },
                            {"value": [1]
                             }
                        ]
                    },
                    {
                        "function": "in",
                        "args": [{
                            "var": "categorical_var",
                            "axes": ["response_3"]
                        },
                            {"value": [1]
                             }
                        ]
                    }
                ]
            }
        finally:
            # cleanup
            ds.delete()

    def test_categorical_array_any_w_bracket_subvar(self):
        ds = self.site.datasets.create(as_entity({"name": "test_any_categorical_w_bracket_add_filter"})).refresh()
        cat_var = ds.variables.create(as_entity({
            "name": "Categorical Var",
            "alias": "categorical_var",
            "type": "categorical_array",
            "categories": [
                {
                    'id': 1,
                    'missing': False,
                    'name': 'Very interested',
                    'numeric_value': 1
                },
                {
                    'id': 2,
                    'missing': False,
                    'name': 'Somewhat interested',
                    'numeric_value': 2
                },
                {
                    'id': 3,
                    'missing': False,
                    'name': 'A little interested',
                    'numeric_value': 3
                },
                {
                    'id': -1,
                    'missing': True,
                    'name': 'No Data',
                    'numeric_value': None
                }
            ],
            'subvariables': [
                {
                    'alias': 'response_1',
                    'name': 'Response1'
                },
                {
                    'alias': 'response_2',
                    'name': 'Response2'
                },
                {
                    'alias': 'response_3',
                    'name': 'Response3'
                },
            ],
            "values": [
                [1, 3, -1],
                [2, 1, 1],
                [2, 3, 2]
            ]
        }))
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        _filter = "categorical_var[response_1].any([1])"
        try:
            resp = scrunch_dataset.add_filter(name='filter_1', expr=_filter)
            data = ds.follow("table", "limit=20&filter={}".format(resp.resource.self))['data']
            ds_variables = ds.variables.by("alias")
            cat_var_variable_id = ds_variables["categorical_var"].id
            assert data[cat_var_variable_id] == [
                [1, 3, {"?": -1}]
            ]
            assert resp.resource.body.entity.body.definition == {
                "function": "in",
                "args": [
                    {
                        "var": "categorical_var",
                        "axes": ["response_1"]
                    },
                    {
                        "value": [1]
                    }
                ]
            }
        finally:
            # cleanup
            ds.delete()

    def test_append_dataset_any_filter_multiple_response(self):
        ds_rows = [
            ["response_1", "response_2", "response_3"],
            [1, 2, 1],
            [1, 2, 2],
            [1, 1, 1]
        ]
        ds_to_append_rows = [
            ["response_1", "response_2", "response_3"],
            [2, 1, 2],
            [2, 2, 1],
            [2, 2, 1]
        ]
        ds, scrunch_dataset = self._create_mr_dataset('test_mr_any_subvar', ds_rows)
        ds_to_append, scrunch_dataset_to_append = self._create_mr_dataset(
            'test_mr_any_to_append_subvar',
            ds_to_append_rows
        )
        # This filter should get only the rows that have the mr_response variable with the value 1
        # at the same time for both response_1 and response_2
        _filter = "mr_variable.any([response_1, response_2])"
        try:
            scrunch_dataset.append_dataset(scrunch_dataset_to_append, filter=_filter)
            ds_variables = ds.variables.by("alias")
            mr_variable_id = ds_variables["mr_variable"].id
            data = ds.follow("table", "limit=20")['data']
            assert data[mr_variable_id] == [
                [1, 2, 1],
                [1, 2, 2],
                [1, 1, 1],
                [2, 1, 2],
            ]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    def test_append_dataset_any_filter_multiple_response_single_subvar(self):
        ds_rows = [
            ["response_1", "response_2", "response_3"],
            [1, 2, 1],
            [1, 2, 2],
            [1, 1, 1]
        ]
        ds_to_append_rows = [
            ["response_1", "response_2", "response_3"],
            [1, 1, 2],
            [2, 2, 1],
            [1, 1, 1]
        ]
        ds, scrunch_dataset = self._create_mr_dataset('test_mr_any_subvar', ds_rows)
        ds_to_append, scrunch_dataset_to_append = self._create_mr_dataset(
            'test_mr_any_to_append_subvar',
            ds_to_append_rows
        )
        # This filter should get only the rows that have the mr_response variable with the value 1 (selected)
        # for response_1 (not the 2nd row in this test)
        _filter = "mr_variable.any([response_1])"
        try:
            scrunch_dataset.append_dataset(scrunch_dataset_to_append, filter=_filter)
            ds_variables = ds.variables.by("alias")
            mr_variable_id = ds_variables["mr_variable"].id
            data = ds.follow("table", "limit=20")['data']
            assert data[mr_variable_id] == [
                [1, 2, 1],
                [1, 2, 2],
                [1, 1, 1],
                [1, 1, 2],
                [1, 1, 1]
            ]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    def test_categorical_any_add_filter_value(self):
        ds = self.site.datasets.create(as_entity({"name": "test_any_categorical_filter"})).refresh()
        categories = [
            {"id": 1, "name": "One", "missing": False, "numeric_value": None},
            {"id": 2, "name": "Two", "missing": False, "numeric_value": None},
            {"id": 3, "name": "Three", "missing": False, "numeric_value": None},
            {"id": -1, "name": "No Data", "missing": True, "numeric_value": None}
        ]

        ds.variables.create(as_entity({
            "name": "my_cat",
            "alias": "my_cat",
            "type": "categorical",
            "categories": categories,
            "values": [1, -1, 2, 3, 3, 1]
        }))

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        _filter = "my_cat.any([1])"
        try:
            scrunch_dataset.add_filter(name='filter_1', expr=_filter)
            # Adding the filter as exclusion. In this case, we are expecting the opposite
            # of the filter since it is an exclusion one
            scrunch_dataset.exclude(_filter)
            data = ds.follow("table", "limit=20")['data']
            ds_variables = ds.variables.by("alias")
            variable_id = ds_variables["my_cat"].id
            assert data[variable_id] == [{'?': -1}, 2, 3, 3]
        finally:
            # cleanup
            ds.delete()

    def test_categorical_any_add_filter_multiple_values(self):
        ds = self.site.datasets.create(as_entity({"name": "test_any_categorical_filter_multiple_values"})).refresh()
        categories = [
            {"id": 1, "name": "One", "missing": False, "numeric_value": None},
            {"id": 2, "name": "Two", "missing": False, "numeric_value": None},
            {"id": 3, "name": "Three", "missing": False, "numeric_value": None},
            {"id": -1, "name": "No Data", "missing": True, "numeric_value": None}
        ]

        ds.variables.create(as_entity({
            "name": "my_cat",
            "alias": "my_cat",
            "type": "categorical",
            "categories": categories,
            "values": [1, -1, 2, 3, 3, 1]
        }))

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        _filter = "my_cat.any([1, 3])"
        try:
            resp = scrunch_dataset.add_filter(name='filter_1', expr=_filter)
            data = ds.follow("table", "limit=20&filter={}".format(resp.resource.self))['data']
            ds_variables = ds.variables.by("alias")
            cat_var_variable_id = ds_variables["my_cat"].id
            assert data[cat_var_variable_id] == [1, 3, 3, 1]
        finally:
            # cleanup
            ds.delete()
