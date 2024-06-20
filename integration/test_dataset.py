# coding: utf-8

import os
from unittest import TestCase

from pycrunch.importing import Importer

from integration.fixtures import NEWS_DATASET, MR_CATS
from scrunch import connect, get_dataset
from scrunch.mutable_dataset import get_mutable_dataset

HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']


site = connect(username, password, HOST)
assert site is not None, "Unable to connect to %s" % HOST

as_entity = lambda b: {
    "element": "shoji:entity",
    "body": b
}


class TestDatasetMethods(TestCase):
    def test_replace_values(self):
        ds = site.datasets.create(as_entity({"name": "test_replace_values"})).refresh()
        variable = ds.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5],
                }
            )
        ).refresh()
        scrunch_dataset = get_mutable_dataset(ds.body.id, site)
        resp = scrunch_dataset.replace_values({
            "my_var": 4
        }, filter="missing(my_var)")
        if resp is not None:
            # We got a 202 response. Scrunch should have waited for the
            # progress to finish
            progress_url = resp.payload["value"]
            progress = site.session.get(progress_url)
            progress_status = progress.payload["value"]
            assert (
                # Check for new or old complete task message
                progress_status == {'progress': 100, 'message': 'complete'}
                or progress_status == {'progress': 100, 'message': 'completed'}
            )
        else:
            # This means the API handled this synchronously. 204 response
            pass

        r = ds.follow("table", "limit=10")["data"]
        try:
            assert r[variable.body["id"]] == [1, 2, 3, 4, 5]
        finally:
            ds.delete()

    def test_append_dataset_any_filter(self):
        _dataset_metadata = {
            "caseid": {
                "name": "Case ID",
                "type": "numeric"
            },
            "age": {
                "name": "Age",
                "type": 'numeric',
            },
            "newssource": {
                "name": "News source",
                "type": "multiple_response",
                "categories": MR_CATS,
                "subreferences": [{
                    "name": "Facebook",
                    "alias": "newssource_1"
                }, {
                    "name": "Twitter",
                    "alias": "newssource_2"
                }, {
                    "name": "Google news",
                    "alias": "newssource_3"
                }]
            },
        }
        ds = site.datasets.create({
            'element': 'shoji:entity',
            'body': {
                'name': 'test_mr_any',
                'table': {
                    'element': 'crunch:table',
                    'metadata': _dataset_metadata
                },
            }
        }).refresh()
        ds_to_append = site.datasets.create({
            'element': 'shoji:entity',
            'body': {
                'name': 'test_append_mr_any',
                'table': {
                    'element': 'crunch:table',
                    'metadata': _dataset_metadata
                },
            }
        }).refresh()
        ds_rows = [
            ["caseid", "age", "newssource_1", "newssource_2", "newssource_3"],
            [1, 25, 1, 2, 1],
            [2, 41, 1, 2, 2],
            [3, 33, 1, 1, 1]
        ]
        ds_to_append_rows = [
            ["caseid", "age", "newssource_1", "newssource_2", "newssource_3"],
            [4, 10, 2, 1, 2],
            [5, 11, 2, 1, 1],
            [6, 12, 1, 2, 2]
        ]
        Importer().append_rows(ds, ds_rows)
        Importer().append_rows(ds_to_append, ds_to_append_rows)
        scrunch_dataset = get_mutable_dataset(ds.body.id, site)
        scrunch_dataset_to_append = get_mutable_dataset(ds_to_append.body.id, site)
        try:
            scrunch_dataset.append_dataset(scrunch_dataset_to_append, filter="newssource.any([1])")
            ds_variables = ds.variables.by("alias")
            age_variable_id = ds_variables["age"].id
            case_id_variable_id = ds_variables["caseid"].id
            newssource_variable_id = ds_variables["newssource"].id
            data = ds.follow("table", "limit=20")['data']
            assert data[case_id_variable_id] == [1.0, 2.0, 3.0, 6.0]
            assert data[age_variable_id] == [25.0, 41.0, 33.0, 12.0]
            assert data[newssource_variable_id] == [[1, 2, 1], [1, 2, 2], [1, 1, 1], [1, 2, 2]]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()


class TestCategories(TestCase):
    def test_edit_category(self):
        ds = site.datasets.create(as_entity({"name": "test_edit_category"})).refresh()

        categories = [
            {"id": 1, "name": "One", "missing": False, "numeric_value": None},
            {"id": 2, "name": "Two", "missing": False, "numeric_value": None},
            {"id": -1, "name": "No Data", "missing": True, "numeric_value": None}
        ]

        my_cat = ds.variables.create(as_entity({
            "name": "my_cat",
            "alias": "my_cat",
            "type": "categorical",
            "categories": categories
        }))

        scrunch_dataset = get_mutable_dataset(ds.body.id, site)
        my_cat = scrunch_dataset[my_cat.body["alias"]]  # Ensure refreshed var
        my_cat.categories[1].edit(numeric_value=1)

        my_cat_reloaded = scrunch_dataset[my_cat.alias]
        try:
            assert my_cat_reloaded.categories[1].as_dict() == dict(categories[0], numeric_value=1, selected=False)
            assert my_cat_reloaded.categories[2].as_dict() == dict(categories[1], selected=False)
        finally:
            ds.delete()
