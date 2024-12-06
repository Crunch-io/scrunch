# coding: utf-8
import csv
import os
import tempfile
import uuid
from datetime import datetime

import numpy
import pyspssio
from fixtures import BaseIntegrationTestCase
from numpy.testing import assert_equal as numpy_assert_equal
from pycrunch.shoji import Entity, as_entity

from scrunch.cubes import crtabs
from scrunch.datasets import Project
from scrunch.streaming_dataset import StreamingDataset

PROJECT_ID = os.environ.get("SCRUNCH_PROJECT_ID")
PROJECT_311_ID = os.environ.get("SCRUNCH_PROJECT_311_ID")
TEST_DATASET_ID = os.environ.get("SCRUNCH_TEST_DATASET_ID")


class BaseTestCase(BaseIntegrationTestCase):
    TEST_FUNCTIONS = []
    _created_datasets = None
    weight = None

    def _export_dataset(self, ds, format_="csv"):
        output = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
        ds.export(output.name, format=format_)
        return self._parse_dataset_export(output, format_)

    def _parse_dataset_export(self, output, format_="csv"):
        if format_ == "csv":
            reader = csv.DictReader(output)

            # put the data into columns
            actual = {}
            for row in reader:
                for k, v in row.items():
                    actual.setdefault(k, []).append(v)
            return {k: [o.strip() for o in v] for k, v in actual.items()}
        elif format_ == "spss":
            data, metadata = pyspssio.read_sav(output.name)
            return {k: list(data.get(k)) for k in metadata["var_names"]}

    def setUp(self):
        self._created_datasets = {}
        super().setUp()

    def tearDown(self):
        for ds, views in self._created_datasets.values():
            for view in views.values():
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
        return project

    def _log(self, msg):
        print(msg)

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
            name = "Weighed test view dataset" if self.weight else "Test view dataset"
        ds_data["name"] = (
            f"{name} {uuid.uuid4().hex[:16]} [{datetime.now().isoformat()}]"
        )
        project_id = PROJECT_311_ID if on_311 else PROJECT_ID
        if project_id:
            ds_data["project"] = f"/projects/{project_id}/"
        view = self.site.datasets.create(as_entity(ds_data)).refresh()
        self._created_datasets[ds.self][1][view.self] = view
        if self.weight:
            view.settings.patch(
                {"weight": view.variables.by("alias")[self.weight].entity.self}
            )
        streaming_view = StreamingDataset(view)
        self._log(f"[{streaming_view.id}] {name} [project={project_id}]")
        return streaming_view, view

    def _create_dataset(self, on_311=True, pk=None, **values):
        pk = values.pop("pk", None)
        name = values.pop("name", None)
        ds_data = {k: v for k, v in values.items()}
        if not name:
            name = "Weighed test dataset" if self.weight else "Test dataset"
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
        ds.variables.create(
            as_entity(
                {
                    "name": "weight_var",
                    "alias": "weight_var",
                    "type": "numeric",
                    "values": [10, 8, 14, 10, 12, 9, 10, 11, 9, 7],
                }
            )
        ).refresh()
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
        ds.variables.create(
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
        ds.variables.create(
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
        self._created_datasets[ds.self] = (ds, {})
        if self.weight:
            ds.settings.patch(
                {"weight": ds.variables.by("alias")[self.weight].entity.self}
            )
        streaming_ds = StreamingDataset(ds)
        streaming_ds.set_weight(["my_var"])
        self._log(f"[{streaming_ds.id}] {name} [project={project_id}]")
        return streaming_ds, ds

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
        assert set(view.variable_aliases()) == {"cat1", "der1", "my_var"}
        assert set(view2.variable_aliases()) == {"cat1", "der1", "my_var"}
        view = self._revert_dataset_version(view)
        view2 = self._revert_dataset_version(view2)
        assert set(view.variable_aliases()) == {"cat1", "der1", "my_var"}
        assert set(view2.variable_aliases()) == {"cat1", "der1", "my_var"}

    def _test_cube_query_on_dataset(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        resp = crtabs(dataset=ds, variables=["cat1"], weight=self.weight)
        numpy.testing.assert_array_equal(resp.counts, numpy.array([3, 2, 2]))
        ds = self._change_dataset_version(ds)
        resp = crtabs(dataset=ds, variables=["cat1"], weight=self.weight)
        numpy.testing.assert_array_equal(resp.counts, numpy.array([3, 2, 2]))

    def _test_cube_query_on_view(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        view, view_instance = self._create_view(ds_instance)
        resp = crtabs(dataset=view, variables=["cat1"], weight=self.weight)
        numpy.testing.assert_array_equal(resp.counts, numpy.array([3, 2, 2]))
        ds = self._change_dataset_version(ds)
        view = self._change_dataset_version(view)
        resp = crtabs(dataset=view, variables=["cat1"], weight=self.weight)
        numpy.testing.assert_array_equal(resp.counts, numpy.array([3, 2, 2]))

    def _test_export_dataset(self, format_, expected):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        exported = self._export_dataset(ds, format_)
        numpy_assert_equal(exported, expected)
        ds = self._change_dataset_version(ds)
        exported = self._export_dataset(ds, format_)
        numpy_assert_equal(exported, expected)

    def _test_export_dataset_as_csv(self):
        EXPECTED = {
            "my_var": [
                "1.0",
                "2.0",
                "3.0",
                "No Data",
                "5.0",
                "6.0",
                "7.0",
                "8.0",
                "No Data",
                "11.0",
            ],
            "weight_var": [
                "10.0",
                "8.0",
                "14.0",
                "10.0",
                "12.0",
                "9.0",
                "10.0",
                "11.0",
                "9.0",
                "7.0",
            ],
            "cat1": ["1", "2", "3", "-1", "-1", "-1", "1", "2", "3", "1"],
            "der1": [
                "2.0",
                "3.0",
                "4.0",
                "No Data",
                "6.0",
                "7.0",
                "8.0",
                "9.0",
                "No Data",
                "12.0",
            ],
        }

        self._test_export_dataset("csv", EXPECTED)

    def _test_export_dataset_as_spss(self):
        nan = float("nan")
        EXPECTED = {
            "my_var": [
                1.0,
                2.0,
                3.0,
                nan,
                5.0,
                6.0,
                7.0,
                8.0,
                nan,
                11.0,
            ],
            "weight_var": [
                10.0,
                8.0,
                14.0,
                10.0,
                12.0,
                9.0,
                10.0,
                11.0,
                9.0,
                7.0,
            ],
            "cat1": [1.0, 2.0, 3.0, nan, nan, nan, 1.0, 2.0, 3.0, 1.0],
            "der1": [
                2.0,
                3.0,
                4.0,
                nan,
                6.0,
                7.0,
                8.0,
                9.0,
                nan,
                12.0,
            ],
        }

        self._test_export_dataset("spss", EXPECTED)


class Test36Datasets(BaseTestDatasets):
    CURRENT_VERSION = "3.6"
    TEST_FUNCTIONS = [
        "test_create_dataset",
        "test_switch_dataset",
        "test_add_variable_to_dataset",
        "test_delete_variable_from_dataset",
        "test_dataset_with_view",
        "test_cube_query_on_dataset",
        "test_cube_query_on_view",
        "test_export_dataset_as_csv",
        "test_export_dataset_as_spss",
    ]


class WeightedTest36Datasets(Test36Datasets):
    weight = "weight_var"


class Test311Datasets(BaseTestDatasets):
    CURRENT_VERSION = "3.11"
    TEST_FUNCTIONS = [
        "test_create_dataset",
        "test_switch_dataset",
        "test_add_variable_to_dataset",
        "test_delete_variable_from_dataset",
        "test_dataset_with_view",
        "test_cube_query_on_dataset",
        "test_cube_query_on_view",
        "test_export_dataset_as_csv",
        "test_export_dataset_as_spss",
    ]


class WeightedTest311Datasets(Test311Datasets):
    weight = "weight_var"
