# coding: utf-8

import os
from unittest import TestCase

from scrunch import connect
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
        assert r[variable.body["id"]] == [1, 2, 3, 4, 5]
        ds.delete()


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
        assert my_cat_reloaded.categories[1].as_dict() == dict(categories[0], numeric_value=1, selected=False)
        assert my_cat_reloaded.categories[2].as_dict() == dict(categories[1], selected=False)

