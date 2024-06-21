# coding: utf-8

import os
from unittest import TestCase

from pycrunch.importing import Importer

from integration.fixtures import MR_CATS, as_entity
from scrunch import connect
from scrunch.mutable_dataset import get_mutable_dataset

HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']


class TestExpressions(TestCase):

    SITE = None

    @property
    def site(self):
        if self.SITE is None:
            _site = connect(username, password, HOST)
            assert _site is not None, "Unable to connect to %s" % HOST
            self.SITE = _site
        return self.SITE

    def _create_mr_dataset(self, name, rows):
        _dataset_metadata = {
            "case_id": {
                "name": "Case ID",
                "type": "numeric"
            },
            "age": {
                "name": "Age",
                "type": 'numeric',
            },
            "news_source": {
                "name": "News source",
                "type": "multiple_response",
                "categories": MR_CATS,
                "subreferences": [{
                    "name": "Facebook",
                    "alias": "news_source_1"
                }, {
                    "name": "Twitter",
                    "alias": "news_source_2"
                }, {
                    "name": "Google news",
                    "alias": "news_source_3"
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

    def test_mr_any_on_value(self):
        ds_rows = [
            ["case_id", "age", "news_source_1", "news_source_2", "news_source_3"],
            [1, 25, 1, 2, 1],
            [2, 41, 1, 2, 2],
            [3, 33, 1, 1, 1]
        ]
        ds_to_append_rows = [
            ["case_id", "age", "news_source_1", "news_source_2", "news_source_3"],
            [4, 10, 2, 1, 2],
            [5, 11, 2, 1, 1],
            [6, 12, 1, 2, 2]
        ]
        ds, scrunch_dataset = self._create_mr_dataset('test_mr_any', ds_rows)
        ds_to_append, scrunch_dataset_to_append = self._create_mr_dataset('test_mr_any_to_append', ds_to_append_rows)
        # This filter should get only the rows that have the news_source variable with the value 1
        _filter = "news_source.any([1])"
        try:
            scrunch_dataset.append_dataset(scrunch_dataset_to_append, filter=_filter)
            ds_variables = ds.variables.by("alias")
            age_variable_id = ds_variables["age"].id
            case_id_variable_id = ds_variables["case_id"].id
            news_source_variable_id = ds_variables["news_source"].id
            data = ds.follow("table", "limit=20")['data']
            assert data[case_id_variable_id] == [1.0, 2.0, 3.0, 6.0]
            assert data[age_variable_id] == [25.0, 41.0, 33.0, 12.0]
            assert data[news_source_variable_id] == [[1, 2, 1], [1, 2, 2], [1, 1, 1], [1, 2, 2]]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    def test_mr_any_on_subvar(self):
        ds_rows = [
            ["case_id", "age", "news_source_1", "news_source_2", "news_source_3"],
            [1, 25, 1, 2, 1],
            [2, 41, 1, 2, 2],
            [3, 33, 1, 1, 1]
        ]
        ds_to_append_rows = [
            ["case_id", "age", "news_source_1", "news_source_2", "news_source_3"],
            [4, 10, 1, 1, 2],
            [5, 11, 2, 1, 1],
            [6, 12, 1, 1, 2]
        ]
        ds, scrunch_dataset = self._create_mr_dataset('test_mr_any_subvar', ds_rows)
        ds_to_append, scrunch_dataset_to_append = self._create_mr_dataset('test_mr_any_to_append_subvar',
                                                                          ds_to_append_rows)
        # This filter should get only the rows that have the news_source variable with the value 1
        # at the same time for both news_source_1 and news_source_2
        _filter = "news_source.any([news_source_1, news_source_2])"
        try:
            resp = scrunch_dataset.append_dataset(scrunch_dataset_to_append, filter=_filter)
            ds_variables = ds.variables.by("alias")
            age_variable_id = ds_variables["age"].id
            case_id_variable_id = ds_variables["case_id"].id
            news_source_variable_id = ds_variables["news_source"].id
            data = ds.follow("table", "limit=20")['data']
            assert data[case_id_variable_id] == [1.0, 2.0, 3.0, 4.0, 6.0]
            assert data[age_variable_id] == [25.0, 41.0, 33.0, 11.0, 12.0]
            assert data[news_source_variable_id] == [[1, 2, 1], [1, 2, 2], [1, 1, 1], [1, 1, 2], [1, 1, 2]]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    def test_categorical_any_on_subvar(self):
        ds = self.site.datasets.create(as_entity({"name": "test_any_category"})).refresh()
        ds_to_append = self.site.datasets.create(as_entity({"name": "test_any_category_to_append"})).refresh()
        categories = [
            {"id": 1, "name": "One", "missing": False, "numeric_value": None},
            {"id": 2, "name": "Two", "missing": False, "numeric_value": None},
            {"id": 3, "name": "Three", "missing": False, "numeric_value": None},
            {"id": -1, "name": "No Data", "missing": True, "numeric_value": None}
        ]
        my_cat = ds.variables.create(as_entity({
            "name": "my_cat",
            "alias": "my_cat",
            "type": "categorical",
            "categories": categories,
            "values": [
                1, 2, 3, -1, -1, -1, 1, 2, 1
            ]
        }))
        ds_to_append.variables.create(as_entity({
            "name": "my_cat",
            "alias": "my_cat",
            "type": "categorical",
            "categories": categories,
            "values": [
                1, 3, -1, 2
            ]
        }))
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        scrunch_dataset_to_append = get_mutable_dataset(ds_to_append.body.id, self.site)

        # This filter should get only the rows that have the news_source variable with the value 1
        # at the same time for both news_source_1 and news_source_2
        _filter = "my_cat.any([1])"
        try:
            resp = scrunch_dataset.append_dataset(scrunch_dataset_to_append, filter=_filter)
            ds_variables = ds.variables.by("alias")
            data = ds.follow("table", "limit=20")['data']
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()
