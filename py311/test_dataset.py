# coding: utf-8
import os
import uuid
from datetime import datetime

import pytest
from fixtures import BaseIntegrationTestCase
from pycrunch.shoji import Entity, as_entity

from scrunch.datasets import Project
from scrunch.mutable_dataset import get_mutable_dataset
from scrunch.streaming_dataset import StreamingDataset

PROJECT_ID = os.environ.get("SCRUNCH_PROJECT_ID")
PROJECT_311_ID = os.environ.get("SCRUNCH_PROJECT_311_ID")
TEST_DATASET_ID = os.environ.get("SCRUNCH_TEST_DATASET_ID")


class BaseTestCase(BaseIntegrationTestCase):
    TEST_FUNCTIONS = []
    _created_datasets = None

    def setUp(self):
        self._created_datasets = {}
        super().setUp()

    def tearDown(self):
        for ds, views in self._created_datasets.values():
            for view in views: 
                view.delete()
            ds.delete()

        super().tearDown()

    def _project(self, id):
        project = Project(
            Entity(
                self.site.session,
                **{
                    "self": f"{self.site.self}projects/{id}/",
                    "element": "shoji:entity",
                    "body": {"name": "Target project"},
                },
            )
        )
        # project.move_here = MagicMock()
        return project

    def _change_dataset_version(self, ds):
        project_id = PROJECT_311_ID if self.CURRENT_VERSION == "3.6" else PROJECT_ID
        ds.move(self._project(project_id))
        return ds

    def _revert_dataset_version(self, ds):
        project_id = PROJECT_ID if self.CURRENT_VERSION == "3.6" else PROJECT_311_ID
        ds.move(self._project(project_id))
        return ds

    def _create_view(self, ds, on_311=True, **values):
        ds_data = {k: v for k, v in values.items()}
        name = values.pop("name", None)
        ds_data["view_of"] = ds.self
        if not name:
            name = "Test view dataset"
        ds_data["name"] = (
            f"{name} {uuid.uuid4().hex[:16]} [{datetime.now().isoformat()}]"
        )
        project_id = PROJECT_311_ID if on_311 else PROJECT_ID
        if project_id:
            ds_data["project"] = f"/projects/{project_id}/"
        view = self.site.datasets.create(as_entity(ds_data)).refresh()
        self._created_datasets[ds.self][1].add(view)
        return StreamingDataset(view), view

    def _create_dataset(self, on_311=True, pk=None, **values):
        pk = values.pop("pk", None)
        name = values.pop("name", None)
        ds_data = {k: v for k, v in values.items()}
        if not name:
            name = "Test dataset"
        ds_data["name"] = (
            f"{name} {uuid.uuid4().hex[:16]} [{datetime.now().isoformat()}]"
        )
        project_id = PROJECT_311_ID if on_311 else PROJECT_ID
        if project_id:
            ds_data["project"] = f"/projects/{project_id}/"
        ds = self.site.datasets.create(as_entity(ds_data)).refresh()
        if pk:
            ds.variables.create(
                as_entity(
                    {
                        "name": "pk",
                        "alias": "pk",
                        "type": "numeric",
                        "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    }
                )
            )
        var1 = ds.variables.create(
            as_entity(
                {
                    "name": "my_var",
                    "alias": "my_var",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5, 6, 7, 8, None, 11],
                }
            )
        ).refresh()
        var2 = ds.variables.create(
            as_entity(
                {
                    "name": "cat1",
                    "alias": "cat1",
                    "type": "categorical",
                    "categories": [
                        {
                            "id": 1,
                            "name": "cat 1",
                            "missing": False,
                            "numeric_value": None,
                        },
                        {
                            "id": 2,
                            "name": "cat 2",
                            "missing": False,
                            "numeric_value": None,
                        },
                        {
                            "id": 3,
                            "name": "cat 3",
                            "missing": False,
                            "numeric_value": None,
                        },
                        {
                            "id": -1,
                            "name": "No Data",
                            "missing": True,
                            "numeric_value": None,
                        },
                    ],
                    "values": [1, 2, 3, -1, -1, -1, 1, 2, 3, 1],
                }
            )
        )
        var3 = ds.variables.create(
            as_entity(
                {
                    "name": "der1",
                    "alias": "der1",
                    "derived": True,
                    "derivation": {
                        "function": "+",
                        "args": [
                            {"variable": var1.self},
                            {"value": 1},
                        ],
                    },
                }
            )
        )
        self._created_datasets[ds.self] = (ds, set())
        return StreamingDataset(ds), ds

    def __new__(cls, *args, **kwargs):
        for fn_name in cls.TEST_FUNCTIONS:
            if hasattr(cls, fn_name):
                continue
            orig_fn = getattr(cls, f"_{fn_name}", None)
            if not orig_fn:
                continue
            setattr(cls, fn_name, orig_fn)
        return super().__new__(cls)


class BaseTestDatasets(BaseTestCase):
    def _test_create_dataset(self):
        ds, _ = self._create_dataset(name="test_dataset")
        assert set(ds.variable_aliases()) == {"cat1", "der1", "my_var"}

    def _test_switch_dataset(self):
        ds, _ = self._create_dataset(name="test_dataset")
        ds = self._change_dataset_version(ds)
        assert set(ds.variable_aliases()) == {"cat1", "der1", "my_var"}

    def _test_add_variable_to_dataset(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        ds = self._change_dataset_version(ds)
        var1 = ds_instance.variables.by("alias")["my_var"].entity
        ds_instance.variables.create(
            as_entity(
                {
                    "name": "der2",
                    "alias": "der2",
                    "derived": True,
                    "derivation": {
                        "function": "+",
                        "args": [
                            {"variable": var1.self},
                            {"value": 2},
                        ],
                    },
                }
            )
        )
        assert set(ds.variable_aliases()) == {"cat1", "der1", "der2", "my_var"}

    def _test_delete_variable_from_dataset(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        ds = self._change_dataset_version(ds)
        der1 = ds_instance.variables.by("alias")["der1"].entity
        der1.delete()
        assert set(ds.variable_aliases()) == {"cat1", "my_var"}
        ds = self._revert_dataset_version(ds)
        assert set(ds.variable_aliases()) == {"cat1", "my_var"}

    def _test_dataset_with_view(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        view, view_instance = self._create_view(ds_instance)
        ds = self._change_dataset_version(ds)
        view = self._change_dataset_version(view)
        view2, view2_instance = self._create_view(ds_instance)
        assert set(view.variable_aliases()) == {"cat1", "my_var"}
        assert set(view2.variable_aliases()) == {"cat1", "my_var"}
        view = self._revert_dataset_version(view)
        view2 = self._revert_dataset_version(view2)
        assert set(view.variable_aliases()) == {"cat1", "my_var"}
        assert set(view2.variable_aliases()) == {"cat1", "my_var"}


class Test36Datasets(BaseTestDatasets):
    CURRENT_VERSION = "3.6"
    TEST_FUNCTIONS = [
        "test_create_dataset",
        "test_switch_dataset",
        "test_add_variable_to_dataset",
        "test_delete_variable_from_dataset",
        "test_dataset_with_view",
    ]


class Test311Datasets(BaseTestDatasets):
    CURRENT_VERSION = "3.11"
    TEST_FUNCTIONS = [
        "test_create_dataset",
        "test_switch_dataset",
        "test_add_variable_to_dataset",
        "test_delete_variable_from_dataset",
        "test_dataset_with_view",
    ]
