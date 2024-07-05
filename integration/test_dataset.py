# coding: utf-8
import os


import pytest
from pycrunch.shoji import as_entity

from scrunch.mutable_dataset import get_mutable_dataset
from fixtures import BaseIntegrationTestCase


class TestDatasetMethods(BaseIntegrationTestCase):
    def test_replace_values(self):
        ds = self.site.datasets.create(as_entity({"name": "test_replace_values"})).refresh()
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
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        resp = scrunch_dataset.replace_values({
            "my_var": 4
        }, filter="missing(my_var)")
        if resp is not None:
            # We got a 202 response. Scrunch should have waited for the
            # progress to finish
            progress_url = resp.payload["value"]
            progress = self.site.session.get(progress_url)
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

    @pytest.mark.skipif(os.environ.get("LOCAL_INTEGRATION") is not None, reason="Do not run this test during CI/CD")
    def test_append_dataset(self):
        ds = self.site.datasets.create(as_entity({"name": "test_scrunch_append_dataset"})).refresh()
        ds.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5],
                }
            )
        ).refresh()
        datetime_var = ds.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2023-01-03T20:00:52.333",
                        "2023-02-03T20:00:52.234",
                        "2023-03-03T20:00:52.456",
                        "2023-04-03T20:00:52.999",
                        "2023-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()
        ds_to_append = self.site.datasets.create(as_entity({"name": "test_scrunch_dataset_to_append"})).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [11, 21, 31, 41, 51],
                }
            )
        ).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2024-01-03T20:00:52.333",
                        "2024-02-03T20:00:52.234",
                        "2024-03-03T20:00:52.456",
                        "2024-04-03T20:00:52.999",
                        "2024-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        scrunch_dataset_to_append = get_mutable_dataset(ds_to_append.body.id, self.site)
        try:
            scrunch_dataset.append_dataset(scrunch_dataset_to_append)
            data = ds.follow("table", "limit=20")['data']
            datetime_values = data[datetime_var.body.id]
            # We should have 5 (original dataset) + 5 (from the append_dataset)
            assert len(datetime_values) == 10
            assert datetime_values == [
                "2023-01-03T20:00:52.333",
                "2023-02-03T20:00:52.234",
                "2023-03-03T20:00:52.456",
                "2023-04-03T20:00:52.999",
                "2023-06-03T20:00:52.123",
                "2024-01-03T20:00:52.333",
                "2024-02-03T20:00:52.234",
                "2024-03-03T20:00:52.456",
                "2024-04-03T20:00:52.999",
                "2024-06-03T20:00:52.123",
            ]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    @pytest.mark.skipif(os.environ.get("LOCAL_INTEGRATION") is not None, reason="Do not run this test during CI/CD")
    def test_append_dataset_with_filter(self):
        ds = self.site.datasets.create(as_entity({"name": "test_scrunch_append_dataset"})).refresh()
        ds.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5],
                }
            )
        ).refresh()
        datetime_var = ds.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2023-01-03T20:00:52.333",
                        "2023-02-03T20:00:52.234",
                        "2023-03-03T20:00:52.456",
                        "2023-04-03T20:00:52.999",
                        "2023-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()
        ds_to_append = self.site.datasets.create(as_entity({"name": "test_scrunch_dataset_to_append"})).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [11, 21, 31, 41, 51],
                }
            )
        ).refresh()
        datetime_append_var = ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2024-01-03T20:00:52.333",
                        "2024-02-03T20:00:52.234",
                        "2024-03-03T20:00:52.456",
                        "2024-04-03T20:00:52.999",
                        "2024-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        scrunch_dataset_to_append = get_mutable_dataset(ds_to_append.body.id, self.site)
        # This is intended to leave only two records. Changing the variable `datetime_var`
        # above also changes the test's results
        filter_value = "2024-03-15T00:00:00.393"
        try:
            resp = scrunch_dataset.append_dataset(
                scrunch_dataset_to_append,
                filter="my_datetime_var > '{}'".format(filter_value)
            )
            assert resp['body']['filter'] == {
                'args': [
                    {
                        'variable': datetime_append_var['self']
                    }, {
                        'value': filter_value
                    }
                ],
                'function': '>'
            }
            data = ds.follow("table", "limit=20")['data']
            datetime_values = data[datetime_var.body.id]
            # We should have 5 (original dataset) + 2 (filtered in append_dataset)
            assert len(datetime_values) == 7
            assert datetime_values == [
                "2023-01-03T20:00:52.333",
                "2023-02-03T20:00:52.234",
                "2023-03-03T20:00:52.456",
                "2023-04-03T20:00:52.999",
                "2023-06-03T20:00:52.123",
                "2024-04-03T20:00:52.999",
                "2024-06-03T20:00:52.123",
            ]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    @pytest.mark.skipif(os.environ.get("LOCAL_INTEGRATION") is not None, reason="Do not run this test during CI/CD")
    def test_append_dataset_with_filter_and_exclusion(self):
        ds = self.site.datasets.create(as_entity({"name": "test_scrunch_append_dataset_with_filter_exclusion"})).refresh()
        ds.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5],
                }
            )
        ).refresh()
        datetime_var = ds.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2023-01-03T20:00:52.333",
                        "2023-02-03T20:00:52.234",
                        "2023-03-03T20:00:52.456",
                        "2023-04-03T20:00:52.999",
                        "2023-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()
        ds_to_append = self.site.datasets.create(
            as_entity(
                {
                    "name": "test_scrunch_dataset_with_filter_exclusion_to_append"
                }
            )
        ).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [11, 21, 31, 41, 51],
                }
            )
        ).refresh()
        datetime_append_var = ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2024-01-03T20:00:52.333",
                        "2024-02-03T20:00:52.234",
                        "2024-03-03T20:00:52.456",
                        "2024-04-03T20:00:52.999",
                        "2024-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        scrunch_dataset_to_append = get_mutable_dataset(ds_to_append.body.id, self.site)

        # The exclusion is applied to the dataset to append and here
        # the latest/oldest value is affected
        exclusion = "my_datetime_var >= '2024-06-01T00:00:52.123'"
        scrunch_dataset_to_append.exclude(exclusion)

        # This is intended to leave only two records. Since the one of them is also
        # affected by the exclusion filter, the result should be only one record.
        filter_value = "2024-03-15T00:00:00.393"
        try:
            scrunch_dataset.append_dataset(
                scrunch_dataset_to_append,
                filter="my_datetime_var > '{}'".format(filter_value)
            )
            data = ds.follow("table", "limit=20")['data']
            datetime_values = data[datetime_var.body.id]
            # We should have 5 (original dataset) + 1 (filtered in append_dataset)
            assert len(datetime_values) == 6
            assert datetime_values == [
                "2023-01-03T20:00:52.333",
                "2023-02-03T20:00:52.234",
                "2023-03-03T20:00:52.456",
                "2023-04-03T20:00:52.999",
                "2023-06-03T20:00:52.123",
                "2024-04-03T20:00:52.999",
            ]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    @pytest.mark.skipif(os.environ.get("LOCAL_INTEGRATION") is not None, reason="Do not run this test during CI/CD")
    def test_append_dataset_with_variables_list_and_exclusion(self):
        ds = self.site.datasets.create(as_entity({"name": "test_scrunch_append_dataset_with_variable_exclusion"})).refresh()
        ds.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5],
                }
            )
        ).refresh()
        datetime_var = ds.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2023-01-03T20:00:52.333",
                        "2023-02-03T20:00:52.234",
                        "2023-03-03T20:00:52.456",
                        "2023-04-03T20:00:52.999",
                        "2023-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()
        ds_to_append = self.site.datasets.create(
            as_entity(
                {
                    "name": "test_scrunch_dataset_with_exclusion_to_append"
                }
            )
        ).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [11, 21, 31, 41, 51],
                }
            )
        ).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2024-01-03T20:00:52.333",
                        "2024-02-03T20:00:52.234",
                        "2024-03-03T20:00:52.456",
                        "2024-04-03T20:00:52.999",
                        "2024-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        scrunch_dataset_to_append = get_mutable_dataset(ds_to_append.body.id, self.site)

        # The exclusion is applied to the dataset to append and here
        # the latest/oldest value is affected
        exclusion = "my_datetime_var >= '2024-06-01T00:00:52.123'"
        scrunch_dataset_to_append.exclude(exclusion)
        try:
            scrunch_dataset.append_dataset(
                scrunch_dataset_to_append,
                variables=["my_var", "my_datetime_var"]
            )
            data = ds.follow("table", "limit=20")['data']
            datetime_values = data[datetime_var.body.id]
            # We should have 5 (original dataset) + 4 (filtered by exclusion in append_dataset)
            assert len(datetime_values) == 9
            assert datetime_values == [
                "2023-01-03T20:00:52.333",
                "2023-02-03T20:00:52.234",
                "2023-03-03T20:00:52.456",
                "2023-04-03T20:00:52.999",
                "2023-06-03T20:00:52.123",
                "2024-01-03T20:00:52.333",
                "2024-02-03T20:00:52.234",
                "2024-03-03T20:00:52.456",
                "2024-04-03T20:00:52.999",
            ]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()

    @pytest.mark.skipif(os.environ.get("LOCAL_INTEGRATION") is not None, reason="Do not run this test during CI/CD")
    def test_append_dataset_with_variables_list_filters_and_exclusion(self):
        ds = self.site.datasets.create(as_entity({
            "name": "test_scrunch_append_dataset_with_variable_filters_exclusion"
        })).refresh()
        ds.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5],
                }
            )
        ).refresh()
        datetime_var = ds.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2023-01-03T20:00:52.333",
                        "2023-02-03T20:00:52.234",
                        "2023-03-03T20:00:52.456",
                        "2023-04-03T20:00:52.999",
                        "2023-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()
        ds_to_append = self.site.datasets.create(
            as_entity(
                {
                    "name": "test_scrunch_dataset_with_exclusion_to_append"
                }
            )
        ).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [11, 21, 31, 41, 51],
                }
            )
        ).refresh()
        ds_to_append.variables.create(
            as_entity(
                {
                    "name": "my_datetime_var",
                    "alias": "my_datetime_var",
                    "type": "datetime",
                    "resolution": "ms",
                    "values": [
                        "2024-01-03T20:00:52.333",
                        "2024-02-03T20:00:52.234",
                        "2024-03-03T20:00:52.456",
                        "2024-04-03T20:00:52.999",
                        "2024-06-03T20:00:52.123",
                    ],
                }
            )
        ).refresh()

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        scrunch_dataset_to_append = get_mutable_dataset(ds_to_append.body.id, self.site)

        # The exclusion is applied to the dataset to append and here
        # the latest/oldest value is affected
        exclusion = "my_datetime_var >= '2024-06-01T00:00:52.123'"
        scrunch_dataset_to_append.exclude(exclusion)
        # This is intended to leave only two records.Since the one of them is also
        # affected by the exclusion filter, the result should be only one record.
        filter_value = "2024-03-15T00:00:00.393"
        try:
            scrunch_dataset.append_dataset(
                scrunch_dataset_to_append,
                variables=["my_var", "my_datetime_var"],
                filter="my_datetime_var > '{}'".format(filter_value)
            )
            data = ds.follow("table", "limit=20")['data']
            datetime_values = data[datetime_var.body.id]
            # We should have 5 (original dataset) + 1 (filtered by exclusion and filter in append_dataset)
            assert len(datetime_values) == 6
            assert datetime_values == [
                "2023-01-03T20:00:52.333",
                "2023-02-03T20:00:52.234",
                "2023-03-03T20:00:52.456",
                "2023-04-03T20:00:52.999",
                "2023-06-03T20:00:52.123",
                "2024-04-03T20:00:52.999",
            ]
        finally:
            # cleanup
            ds.delete()
            ds_to_append.delete()


class TestCategories(BaseIntegrationTestCase):
    def test_edit_category(self):
        ds = self.site.datasets.create(as_entity({"name": "test_edit_category"})).refresh()

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

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        my_cat = scrunch_dataset[my_cat.body["alias"]]  # Ensure refreshed var
        my_cat.categories[1].edit(numeric_value=1)

        my_cat_reloaded = scrunch_dataset[my_cat.alias]
        try:
            assert my_cat_reloaded.categories[1].as_dict() == dict(categories[0], numeric_value=1, selected=False)
            assert my_cat_reloaded.categories[2].as_dict() == dict(categories[1], selected=False)
        finally:
            ds.delete()
