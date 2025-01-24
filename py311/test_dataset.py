# coding: utf-8
import codecs
import csv
import os
import tempfile
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Union

import numpy
import pyspssio
from numpy.testing import assert_equal as numpy_assert_equal
from pycrunch.shoji import Entity, as_entity, wait_progress

from integration.fixtures import BaseIntegrationTestCase
from scrunch.cubes import crtabs
from scrunch.datasets import Project
from scrunch.streaming_dataset import StreamingDataset

PROJECT_ID = os.environ.get("SCRUNCH_PROJECT_ID")
PROJECT_311_ID = os.environ.get("SCRUNCH_PROJECT_311_ID")
TEST_DATASET_ID = os.environ.get("SCRUNCH_TEST_DATASET_ID")


class ST:
    """Values the `.type` attribute of a Source object can take"""

    CSV = "csv"
    JSON = "json"
    LDJSON = "ldjson"
    ZCL = "zcl"
    PARQUET = "pqt"
    SPSS = "spss"
    SSS_METADATA = "sss-metadata"
    SSS_DATA = "sss-data"
    CRUNCH_METADATA = "crunch-metadata"
    CSV_TASK_TEST = "csv_task"  # for testing only until csv is moved over to tasks
    SPSS_TASK_TEST = "spss_task"  # for testing only until csv is moved over to tasks


# List of source filetypes from cr.server
source_filetypes = {
    "application/x-ldjson": ST.LDJSON,
    "application/x-spss-sav": ST.SPSS,
    "text/csv": ST.CSV,
    "text/json": ST.JSON,
    "text/ldjson": ST.LDJSON,
    "text/plain": ST.CSV,
    "text/xml": ST.SSS_METADATA,
    "application/x-crunch-metadata+json": ST.CRUNCH_METADATA,
    "application/x-parquet": ST.PARQUET,
    "application/x-ndjson": ST.ZCL,
}

source_mimetypes = {}
for mimetype, val in source_filetypes.items():
    if val not in source_mimetypes:
        source_mimetypes[val] = []
    source_mimetypes[val].append(mimetype)
source_mimetypes["txt"] = ["text/csv"]  # Backward compatibility


def ensure_binary(
    s: Union[str, bytes], encoding: str = "utf-8", errors: str = "strict"
) -> bytes:
    """Coerce **s** to bytes.
    - `str` -> encoded to `bytes`
    - `bytes` -> `bytes`

    :param s: The contents to coerce.
    :param encoding: Encoding type (default to UTF-8).
    :param errors: Error handling level in encoding (default to strict).
    """
    if isinstance(s, bytes):
        return s
    if isinstance(s, str):
        return s.encode(encoding, errors)
    raise TypeError(f"not expecting type '{type(s)}'")


BOUNDARY = "________ThIs_Is_tHe_bouNdaRY_$"


def encode_multipart_formdata(files):
    """Return (content_type, body) ready for httplib.HTTP instance.

    files: a sequence of (name, filename, value) tuples for multipart uploads.
    """
    lines = []
    for key, filename, value in files:
        lines.append("--" + BOUNDARY)
        if filename is None:
            lines.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}')
            continue
        lines.append(
            f'Content-Disposition: form-data; name="{key}"; filename="{filename}"'
        )
        ct = source_mimetypes.get(
            filename.rsplit(".", 1)[-1], ["application/octet-stream"]
        )[0]
        lines.append(f"Content-Type: {ct}")
        lines.append("")
        lines.append(value)
    lines.append("--" + BOUNDARY + "--")
    lines.append("")
    body = "\r\n".join(lines)
    content_type = f"multipart/form-data; charset=UTF-8; boundary={BOUNDARY}"
    return content_type, body


def encode_formdata_item(content, content_type=None, **params):
    body = "--%s\r\n" "Content-Disposition: form-data; %s\r\n" "%s" "\r\n" "%s\r\n"
    params = "; ".join('%s="%s"' % (a, b) for a, b in params.items())

    content_type = "Content-Type: %s\r\n" % content_type if content_type else ""
    return body % (BOUNDARY, params, content_type, content)


class BaseTestCase(BaseIntegrationTestCase):
    TEST_FUNCTIONS = []
    _created_datasets = None
    weight = None

    def _test_file_bytes(self, filename):
        """Return str (bytes) content of test file with *filename*.

        Test files are located in the `tests/files/` directory.
        """
        file_path = os.path.join(filename)
        with codecs.open(file_path, "rb", "latin1") as f:
            contents = f.read()

        return contents.encode().decode("utf8")

    def _encode_file_as_multipart(self, field_name, filename, content_type, contents):
        """Return (content_type, body) containing specified file encoded for upload.

        The returned *content_type* is the "multipart/form-data ..." content-type header
        value for the request, including the boundary string used as a suffix. Note this
        is *not* the same content-type as the file to be uploaded, which is specified as
        a parameter.

        *field_name* is the form field name by which the file will be identified in the
        HTTP request. *filename* should be the OS filename with extension (but no path)
        such as "data.csv". *content_type* is the MIME-type of the file (distinct from
        the `content_type` return value of this method. *contents* is a str (bytes, not
        unicode) containing the content of the file.
        """

        body = encode_formdata_item(
            contents,
            content_type,
            name=field_name,
            filename=filename,
        )
        body += "--%s--\r\n" % BOUNDARY

        content_type = f"multipart/form-data; charset=UTF-8; boundary={BOUNDARY}"

        return content_type, body

    def _parse_on_311(self, on_311: Union[None, bool]) -> bool:
        """
        Based on the value of the parameters, returns True or False, based on whether we are
        meant to run this on a Python 3.11 factory or not.

        This coincides to the value of `on_311` in case it is one of `True` or `False`.
        When the value is `None`, the value corresponds to the current Python version bound with
        the current class - i.e., `True` if `CURRENT_VERSION` is `3.11`, `False` otherwise.
        """
        if on_311 is None:
            return False if self.CURRENT_VERSION == "3.6" else True
        return on_311

    def _import_dataset(
        self,
        metadata: Dict[str, Any],
        input_file: str,
        on_311: Optional[bool] = None,
        format_: str = "csv",
    ):
        """
        :param metadata: The metadata fields associated to the dataset we are creating.
        :param input_file: The input file.
        :param on_311: Whether to run the import under Python 3.11 or not. Default to `None` (same
                       setting as the class).

        """
        on_311 = self._parse_on_311(on_311)
        input_fullpath = os.path.abspath(
            os.path.join(".", "py311", "fixture_files", input_file)
        )
        name = (
            "Weighed imported test dataset" if self.weight else "Imported test dataset"
        )
        ds_data = {k: v for k, v in metadata.items()}
        ds_data["name"] = (
            f"{name} {uuid.uuid4().hex[:16]} [{datetime.now().isoformat()}]"
        )
        project_id = PROJECT_311_ID if on_311 else PROJECT_ID
        if project_id:
            ds_data["project"] = f"/projects/{project_id}/"
        # server/tests/controllers/test_sources.py
        # streaming dataset
        # steps
        # 1. HTTP POST /sources/ {"uploaded_file": Binary}
        # -> response: HTTP 201 - headers {"Location": SourceURL}
        # 2. HTTP POST /datasets/ {"body": {"name": Str}}
        # -> response: HTTP 201 - headers {"Location": DatasetURL}
        # 3. HTTP POST /datasets/DID/batches/ {"savepoint": False, "body":
        # {"workflow": [], "source": SourceURL}}
        # -> response: HTTP 202 (in progress) / headers {"Location": BatchURL}
        # 4. HTTP GET BatchURL
        # -> response: HTTP 200/202 / {"value": {"progress": N, "message":
        # Str}, "views": {"result": NextBatchURL}}
        content_type, body = self._encode_file_as_multipart(
            field_name="uploaded_file",
            filename=input_file,
            content_type=source_mimetypes[format_][0],
            contents=self._test_file_bytes(input_fullpath),
        )

        poster = self.site.sources.post
        content = body
        resp = poster(
            content,
            headers={"Content-Type": content_type, "Content-Length": str(len(body))},
        )
        ds = self.site.datasets.create(as_entity(ds_data)).refresh()
        resp = ds.batches.post(
            {
                "element": "shoji:entity",
                "body": {
                    "source": resp.headers["Location"],
                    "workflow": [],
                },
                "savepoint": False,
            }
        )  # .json()["value"]
        wait_progress(resp, self.site.session)
        return ds.refresh()

    def _export_dataset(self, ds, format_: str = "csv") -> Dict[str, Any]:
        """
        Runs a dataset export.

        :param ds: The dataset.
        :param format_: The export format (one of `csv` and `spss`).
        """
        output = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
        ds.export(output.name, format=format_)
        return self._parse_dataset_export(output, format_)

    def _run_script(self, ds, payload: dict):
        """
        Runs an automation script against a dataset.
        """
        resp = ds.scripts.post(payload)
        assert resp.status_code == 202
        wait_progress(resp, self.site.session)
        return ds.refresh()

    def _parse_dataset_export(self, output: str, format_: str = "csv"):
        """
        Given an output file, parses it and returns the values for it.
        """
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

    def _project(self, id: str) -> Project:
        """
        Returns the scrunch project instance for a specific project ID.
        """
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

    def _log(self, msg: str):
        print(msg)

    def _change_dataset_version(self, ds):
        """
        Switches the current dataset project to the alternative option (i.e., from 3.6 to 3.11, or
        the other way around).
        """
        project_id = PROJECT_311_ID if self.CURRENT_VERSION == "3.6" else PROJECT_ID
        ds.move(self._project(project_id))
        return ds

    def _revert_dataset_version(self, ds):
        """
        Reverts the current dataset project to the original option.
        """
        project_id = PROJECT_ID if self.CURRENT_VERSION == "3.6" else PROJECT_311_ID
        ds.move(self._project(project_id))
        return ds

    def _create_view(self, ds, on_311=None, **values):
        """
        Creates a test view.
        """
        on_311 = self._parse_on_311(on_311)
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

    def _create_dataset(self, on_311=None, pk=None, **values):
        """
        Creates a test dataset.
        """
        on_311 = self._parse_on_311(on_311)
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
                    "name": "A",
                    "alias": "A",
                    "type": "numeric",
                    "values": [1, 2, 3, None, 5, 6, 7, 8, None, 11],
                }
            )
        ).refresh()
        ds.variables.create(
            as_entity(
                {
                    "name": "B",
                    "alias": "B",
                    "type": "numeric",
                    "values": [2, 3, 1, 5, None, 6, 8, 11, 7, None],
                }
            )
        ).refresh()
        ds.variables.create(
            as_entity(
                {
                    "name": "DT",
                    "alias": "DT",
                    "type": "text",
                    "values": [
                        "2024-10-02",
                        "2024-10-03",
                        "2024-10-01",
                        "2024-10-05",
                        None,
                        "2024-09-06",
                        "2024-10-08",
                        "2024-11-11",
                        "2024-10-07",
                        None,
                    ],
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
                    "name": "cat2",
                    "alias": "cat2",
                    "type": "categorical",
                    "categories": [
                        {
                            "id": 1,
                            "name": "cat b1",
                            "missing": False,
                            "numeric_value": None,
                        },
                        {
                            "id": 2,
                            "name": "cat b2",
                            "missing": False,
                            "numeric_value": None,
                        },
                        {
                            "id": 3,
                            "name": "cat b3",
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
                    "values": [1, 3, 2, -1, 1, -1, 1, 2, 3, -1],
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
            ds.preferences.patch(
                {"weight": ds.variables.by("alias")[self.weight].entity.self}
            )
        streaming_ds = StreamingDataset(ds)
        self._log(f"[{streaming_ds.id}] {name} [project={project_id}]")
        return streaming_ds, ds

    def _get_var_values(self, var) -> Dict[str, Any]:
        """
        Given a variable, runs a /dataset/DID/variable/VID/values/ call to get the data values
        associated to it and parses them to return them.
        """
        return self.site.session.get(var.views["values"]).json()["value"]

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
    """
    This class instantiates all the tests we need to run. The actual execution will be
    taken care of by its subclasses, each of them having different settings (i.e., Python version
    and/or weight variable settings).
    """

    def _test_create_dataset(self):
        ds, _ = self._create_dataset(name="test_dataset")
        assert set(ds.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "der1",
            "A",
            "B",
            "weight_var",
        }

    def _test_switch_dataset(self):
        ds, _ = self._create_dataset(name="test_dataset")
        ds = self._change_dataset_version(ds)
        assert set(ds.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "der1",
            "A",
            "B",
            "weight_var",
        }

    def _test_add_variable_to_dataset(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        ds = self._change_dataset_version(ds)
        var1 = ds_instance.variables.by("alias")["A"].entity
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
        assert set(ds.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "der1",
            "der2",
            "A",
            "B",
            "weight_var",
        }

    def _test_delete_variable_from_dataset(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        ds = self._change_dataset_version(ds)
        der1 = ds_instance.variables.by("alias")["der1"].entity
        der1.delete()
        assert set(ds.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "A",
            "B",
            "weight_var",
        }
        ds = self._revert_dataset_version(ds)
        assert set(ds.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "A",
            "B",
            "weight_var",
        }

    def _test_dataset_with_view(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        view, view_instance = self._create_view(ds_instance)
        ds = self._change_dataset_version(ds)
        view = self._change_dataset_version(view)
        view2, view2_instance = self._create_view(ds_instance)
        assert set(view.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "der1",
            "A",
            "B",
            "weight_var",
        }
        assert set(view2.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "der1",
            "A",
            "B",
            "weight_var",
        }
        view = self._revert_dataset_version(view)
        view2 = self._revert_dataset_version(view2)
        assert set(view.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "der1",
            "A",
            "B",
            "weight_var",
        }
        assert set(view2.variable_aliases()) == {
            "DT",
            "cat1",
            "cat2",
            "der1",
            "A",
            "B",
            "weight_var",
        }

    def _assert_cube_query(self, ds):
        EXPECTED = numpy.array([27, 19, 23]) if self.weight else numpy.array([3, 2, 2])
        resp = crtabs(dataset=ds, variables=["cat1"], weight=self.weight)
        numpy.testing.assert_array_equal(resp.counts, EXPECTED)
        ds = self._change_dataset_version(ds)
        resp = crtabs(dataset=ds, variables=["cat1"], weight=self.weight)
        numpy.testing.assert_array_equal(resp.counts, EXPECTED)

    def _test_cube_query_on_dataset(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        return self._assert_cube_query(ds)

    def _test_cube_query_on_view(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        view, view_instance = self._create_view(ds_instance)
        return self._assert_cube_query(view)

    def _test_run_script_change_var_name(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """CHANGE TITLE IN cat1 WITH "Var A";"""
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        assert ds_instance.variables.by("alias")["cat1"].name == "Var A"
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        assert ds_instance.variables.by("alias")["cat1"].name == "Var A"

    def _test_run_script_replace_convert_to_numeric(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        REPLACE CONVERT cat1 TO NUMERIC;
        """
        orig_var = ds_instance.variables.by("alias")["cat1"]
        assert orig_var.get("type") == "categorical"
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["cat1"]
        assert new_var.get("type") == "numeric"
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["cat1"]
        assert new_var.get("type") == "numeric"

    def _test_run_script_replace_convert_to_datetime(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        REPLACE CONVERT DT TO DATETIME FORMAT "%Y-%m-%d";
        """
        orig_var = ds_instance.variables.by("alias")["DT"]
        assert orig_var.get("type") == "text"
        assert self._get_var_values(orig_var.entity) == [
            "2024-10-02",
            "2024-10-03",
            "2024-10-01",
            "2024-10-05",
            {"?": -1},
            "2024-09-06",
            "2024-10-08",
            "2024-11-11",
            "2024-10-07",
            {"?": -1},
        ]
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["DT"]
        assert new_var.get("type") == "datetime"
        assert self._get_var_values(new_var.entity) == [
            "2024-10-02",
            "2024-10-03",
            "2024-10-01",
            "2024-10-05",
            {"?": -1},
            "2024-09-06",
            "2024-10-08",
            "2024-11-11",
            "2024-10-07",
            {"?": -1},
        ]
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["DT"]
        assert new_var.get("type") == "datetime"
        assert self._get_var_values(new_var.entity) == [
            "2024-10-02",
            "2024-10-03",
            "2024-10-01",
            "2024-10-05",
            {"?": -1},
            "2024-09-06",
            "2024-10-08",
            "2024-11-11",
            "2024-10-07",
            {"?": -1},
        ]

    def _test_run_script_replace_convert_to_categorical(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        REPLACE CONVERT A, B TO CATEGORICAL WITH
            VALUE 1 TO "T" CODE 1,
            VALUE 0 TO "F" CODE 2;
        """
        orig_var = ds_instance.variables.by("alias")["A"]
        orig_var_2 = ds_instance.variables.by("alias")["B"]
        assert orig_var.get("type") == "numeric"
        assert orig_var_2.get("type") == "numeric"
        assert orig_var.get("scale") is None
        assert orig_var_2.get("scale") is None
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["A"]
        new_var_2 = ds_instance.variables.by("alias")["B"]
        assert new_var.get("type") == "categorical"
        assert new_var_2.get("type") == "categorical"
        assert new_var.get("scale") == "interval"
        assert new_var_2.get("scale") == "interval"
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["A"]
        new_var_2 = ds_instance.variables.by("alias")["B"]
        assert new_var.get("type") == "categorical"
        assert new_var_2.get("type") == "categorical"
        assert new_var.get("scale") == "interval"
        assert new_var_2.get("scale") == "interval"

    def _test_run_script_replace_convert_to_text(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        REPLACE CONVERT A TO TEXT;
        """
        orig_var = ds_instance.variables.by("alias")["A"]
        assert orig_var.get("type") == "numeric"
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["A"]
        assert new_var.get("type") == "text"
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["A"]
        assert new_var.get("type") == "text"

    def _test_run_script_create_categorical_array(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        CREATE CATEGORICAL ARRAY cat1, cat2 AS array1;
        """
        assert "array1" not in ds_instance.variables.by("alias")
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["array1"]
        assert new_var.get("type") == "categorical_array"
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["array1"]
        assert new_var.get("type") == "categorical_array"

    def _test_run_script_create_categorical_case(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        CREATE CATEGORICAL CASE WHEN
            A == 1 THEN "Cat 1"
            END
            AS A1;
        """
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["A1"]
        assert new_var.get("type") == "categorical"
        resp = self._get_var_values(new_var.entity)
        assert resp == [
            1,
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
        ]
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["A1"]
        assert new_var.get("type") == "categorical"
        resp = self._get_var_values(new_var.entity)
        assert resp == [
            1,
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
            {"?": -1},
        ]

    def _test_run_script_create_categorical_recode(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        CREATE CATEGORICAL RECODE cat1
        MAPPING
            "cat 1", "cat 2" INTO "first two" CODE 1
        AS myrecode;
        """
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["myrecode"]
        assert new_var.get("type") == "categorical"
        assert {
            o["id"]: o["name"] for o in new_var.entity.summary.value["categories"]
        } == {1: "first two", 3: "cat 3", -1: "No Data"}
        resp = self._get_var_values(new_var.entity)
        assert resp == [1, 1, 3, {"?": -1}, {"?": -1}, {"?": -1}, 1, 1, 3, 1]
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["myrecode"]
        assert new_var.get("type") == "categorical"
        assert {
            o["id"]: o["name"] for o in new_var.entity.summary.value["categories"]
        } == {1: "first two", 3: "cat 3", -1: "No Data"}
        resp = self._get_var_values(new_var.entity)
        assert resp == [1, 1, 3, {"?": -1}, {"?": -1}, {"?": -1}, 1, 1, 3, 1]

    def _test_run_script_create_numeric_array(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        CREATE NUMERIC ARRAY A, B AS NumArray;
        """
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["NumArray"]
        assert new_var.get("type") == "numeric_array"
        resp = self._get_var_values(new_var.entity)
        assert resp == [
            [1.0, 2.0],
            [2.0, 3.0],
            [3.0, 1.0],
            [{"?": -1}, 5.0],
            [5.0, {"?": -1}],
            [6.0, 6.0],
            [7.0, 8.0],
            [8.0, 11.0],
            [{"?": -1}, 7.0],
            [11.0, {"?": -1}],
        ]
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["NumArray"]
        assert new_var.get("type") == "numeric_array"
        resp = self._get_var_values(new_var.entity)
        assert resp == [
            [1.0, 2.0],
            [2.0, 3.0],
            [3.0, 1.0],
            [{"?": -1}, 5.0],
            [5.0, {"?": -1}],
            [6.0, 6.0],
            [7.0, 8.0],
            [8.0, 11.0],
            [{"?": -1}, 7.0],
            [11.0, {"?": -1}],
        ]

    def _test_run_script_create_numeric(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        CREATE NUMERIC A + B AS sum;
        """
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["sum"]
        assert new_var.get("type") == "numeric"
        resp = self._get_var_values(new_var.entity)
        assert resp == [
            3.0,
            5.0,
            4.0,
            {"?": -1},
            {"?": -1},
            12.0,
            15.0,
            19.0,
            {"?": -1},
            {"?": -1},
        ]
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["sum"]
        assert new_var.get("type") == "numeric"
        resp = self._get_var_values(new_var.entity)
        assert resp == [
            3.0,
            5.0,
            4.0,
            {"?": -1},
            {"?": -1},
            12.0,
            15.0,
            19.0,
            {"?": -1},
            {"?": -1},
        ]

    def _test_run_script_create_logical(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        CREATE LOGICAL A == 1 AS illogical;
        """
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        new_var = ds_instance.variables.by("alias")["illogical"]
        assert new_var.get("type") == "categorical"
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        new_var = ds_instance.variables.by("alias")["illogical"]
        assert new_var.get("type") == "categorical"

    def _test_run_script_overwrite_numeric_values(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        OVERWRITE NUMERIC VALUES A WITH 0 WHEN cat1 = 1;
        """
        new_var = ds_instance.variables.by("alias")["A"].entity
        resp = self._get_var_values(new_var)
        assert resp == [1.0, 2.0, 3.0, {"?": -1}, 5.0, 6.0, 7.0, 8.0, {"?": -1}, 11.0]
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        resp = self._get_var_values(new_var)
        assert resp == [0, 2.0, 3.0, {"?": -1}, 5.0, 6.0, 0, 8.0, {"?": -1}, 0]
        ds = self._change_dataset_version(ds)
        resp = self._get_var_values(new_var)
        assert resp == [0, 2.0, 3.0, {"?": -1}, 5.0, 6.0, 0, 8.0, {"?": -1}, 0]

    def _test_run_script_set_exclusion(self):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        body = """
        SET EXCLUSION weight_var == 0;
        """
        assert ds.get_exclusion() is None
        ds_instance = self._run_script(
            ds_instance, as_entity({"body": body, "async": False})
        )
        assert ds.get_exclusion() == "weight_var == 0"
        ds = self._change_dataset_version(ds)
        ds_instance = ds_instance.refresh()
        assert ds.get_exclusion() == "weight_var == 0"

    def _test_export_dataset(self, format_, expected):
        ds, ds_instance = self._create_dataset(name="test_dataset")
        exported = self._export_dataset(ds, format_)
        numpy_assert_equal(exported, expected)
        ds = self._change_dataset_version(ds)
        exported = self._export_dataset(ds, format_)
        numpy_assert_equal(exported, expected)

    def _test_export_dataset_as_csv(self):
        EXPECTED = {
            "A": [
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
            "B": [
                "2.0",
                "3.0",
                "1.0",
                "5.0",
                "No Data",
                "6.0",
                "8.0",
                "11.0",
                "7.0",
                "No Data",
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
            "cat2": ["1", "3", "2", "-1", "1", "-1", "1", "2", "3", "-1"],
            "DT": [
                "2024-10-02",
                "2024-10-03",
                "2024-10-01",
                "2024-10-05",
                "No Data",
                "2024-09-06",
                "2024-10-08",
                "2024-11-11",
                "2024-10-07",
                "No Data",
            ],
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
            "A": [
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
            "B": [
                2.0,
                3.0,
                1.0,
                5.0,
                nan,
                6.0,
                8.0,
                11.0,
                7.0,
                nan,
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
            "cat2": [1.0, 3.0, 2.0, nan, 1.0, nan, 1.0, 2.0, 3.0, nan],
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
            "DT": [
                "2024-10-02",
                "2024-10-03",
                "2024-10-01",
                "2024-10-05",
                "-1",
                "2024-09-06",
                "2024-10-08",
                "2024-11-11",
                "2024-10-07",
                "-1",
            ],
        }

        self._test_export_dataset("spss", EXPECTED)

    def _test_import_spss_dataset(self):
        # Still being implemented
        pass

    def _test_import_csv_dataset(self):
        imported_ds = self._import_dataset(
            {"description": "Imported csv dataset"},
            "sample-1-expected.csv",
            format_="csv",
        )
        assert imported_ds.body.description == "Imported csv dataset"
        assert set(imported_ds.variables.by("alias").keys()) == {
            "Q7",
            "Q99",
            "Q6",
            "Q2_5",
            "Q2_4",
            "Q4_9",
            "Q2_9",
            "Q4_3",
            "Q2_1",
            "Q4_1",
            "Q3",
            "Q4_5",
            "Q5",
            "Q4_2",
            "Q2_3",
            "Q2_2",
            "Q4_4",
            "Q1",
        }


#: This is the list of all tests we want to support for integration purposes.
#: This list will grow by time as we implement other ones.
ALL_TEST_FUNCTIONS = [
    "test_create_dataset",
    "test_switch_dataset",
    "test_add_variable_to_dataset",
    "test_delete_variable_from_dataset",
    "test_dataset_with_view",
    "test_cube_query_on_dataset",
    "test_cube_query_on_view",
    "test_export_dataset_as_csv",
    "test_export_dataset_as_spss",
    "test_import_csv_dataset",
    # TODO: still being implemented
    # "test_import_spss_dataset",
    "test_run_script_change_var_name",
    "test_run_script_replace_convert_to_categorical",
    "test_run_script_replace_convert_to_datetime",
    "test_run_script_replace_convert_to_numeric",
    "test_run_script_replace_convert_to_text",
    "test_run_script_set_exclusion",
    "test_run_script_create_categorical_array",
    "test_run_script_create_logical",
    "test_run_script_overwrite_numeric_values",
    "test_run_script_create_numeric",
    "test_run_script_create_numeric_array",
    "test_run_script_create_categorical_case",
    "test_run_script_create_categorical_recode",
]


class Test36Datasets(BaseTestDatasets):
    """
    Dataset tests initially running on a Python 3.6 zz9 factory, no weight variable.
    """

    CURRENT_VERSION = "3.6"
    TEST_FUNCTIONS = ALL_TEST_FUNCTIONS


class WeightedTest36Datasets(Test36Datasets):
    """
    Dataset tests initially running on a Python 3.6 zz9 factory, with weight variable.
    """

    weight = "weight_var"


class Test311Datasets(BaseTestDatasets):
    """
    Dataset tests initially running on a Python 3.11 zz9 factory, no weight variable.
    """

    CURRENT_VERSION = "3.11"
    TEST_FUNCTIONS = ALL_TEST_FUNCTIONS


class WeightedTest311Datasets(Test311Datasets):
    """
    Dataset tests initially running on a Python 3.11 zz9 factory, with weight variable.
    """

    weight = "weight_var"
