# coding: utf-8

import csv
import os
import tempfile
from StringIO import StringIO
from unittest import TestCase

from fixtures import NEWS_DATASET, NEWS_DATASET_ROWS, mr_in, RECODES_CSV_OUTPUT
from scrunch import connect
from scrunch.streaming_dataset import get_streaming_dataset
from scrunch.mutable_dataset import get_mutable_dataset
from pycrunch.importing import Importer


HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']


site = connect(username, password, HOST)
assert site is not None, "Unable to connect to %s" % HOST


class TestBackFill(TestCase):
    def test_backfill_values(self):
        as_entity = lambda b: {
            "element": "shoji:entity",
            "body": b
        }
        ds = site.datasets.create(as_entity({"name": "test_backfill_values"})).refresh()
        # We need a numeric PK
        pk = ds.variables.create(
            as_entity(
                {
                    "name": "pk",
                    "alias": "pk",
                    "type": "numeric",
                    "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                }
            )
        )

        # Create a categorical, note the segment of -1 in rows 4, 5, 6
        cat1 = ds.variables.create(
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
        ).refresh()

        # Create another categorical, note the segment of -1 in rows 4, 5, 6
        cat2 = ds.variables.create(
            as_entity(
                {
                    "name": "cat2",
                    "alias": "cat2",
                    "type": "categorical",
                    "categories": [
                        {
                            "id": 11,
                            "name": "cat 1",
                            "missing": False,
                            "numeric_value": None,
                        },
                        {
                            "id": 22,
                            "name": "cat 2",
                            "missing": False,
                            "numeric_value": None,
                        },
                        {
                            "id": 33,
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
                    "values": [11, 22, 33, -1, -1, -1, 11, 22, 33, 11],
                }
            )
        ).refresh()

        assert False


