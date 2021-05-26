# coding: utf-8

"""
This example shows a more complex example using different recodes and combines
to create new variables.
"""

import csv
import os
import tempfile
from six import StringIO
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


class TestRecodes(TestCase):
    def test_recodes(self):
        raise self.skipTest("Temporarily disabling for API update")
        # Create a dataset for usage
        ds = site.datasets.create({
            'element': 'shoji:entity',
            'body': {
                'name': 'test_recodes',
                'table': {
                    'element': 'crunch:table',
                    'metadata': NEWS_DATASET
                },
                'streaming': 'streaming'
            }
        }).refresh()
        dataset = get_streaming_dataset(ds.body.id, site)
        print("Dataset %s created" % dataset.id)

        # Add data rows
        total = dataset.stream_rows(NEWS_DATASET_ROWS)
        dataset.push_rows(total)

        # Recode a new single response variable
        agerange = dataset.create_categorical([
            {'id': 1, 'name': 'Underage', 'case': 'age < 18'},
            {'id': 2, 'name': 'Millennials', 'case': 'age > 18 and age < 25'},
            {'id': 3, 'name': 'Gen X', 'case': 'age < 35 and age >= 25'},
            {'id': 4, 'name': 'Grown ups', 'case': 'age < 60 and age >= 35'},
            {'id': 5, 'name': '60+', 'case': 'age >= 60'}
        ], alias='agerange', name='Age range', multiple=False)

        print("Variable %s created" % agerange.alias)

        # Recode a new multiple response variable from an existing multiple
        # response variable
        origintype = dataset.create_categorical([
            {'id': 1, 'name': "Online",
             # Mixed support for using "category"(subvariables really) IDs
             'case': mr_in(dataset, 'newssource', [1, 2, 3, 4])},  # Only in the helper
            {'id': 2, 'name': "Print", 'case': mr_in(dataset, 'newssource', [5, 6])},
            {'id': 3, 'name': "Tv", 'case': mr_in(dataset, 'newssource', [7, 9])},
            {'id': 4, 'name': "Radio", 'case': mr_in(dataset, 'newssource', [8, 10])},
        ], alias='origintype', name="News source by type", multiple=True)

        print("Variable %s created" % origintype.alias)

        # Add an exclusion filter
        dataset.exclude('agerange == 1')  # Remove underage

        # MAYBE: Create a new numeric expression based on arbitrary rules

        # Copy a variable
        origintype_copy = dataset.copy_variable(
            origintype,
            name='Copy of origintype',
            alias='origintype_copy')
        print("Variable %s created" % origintype_copy.alias)

        onlinenewssource = dataset.combine_categories(
            origintype_copy,
            map={
                1: 1,
                2: [2, 3, 4]
            },
            categories={
                1: "online",
                2: "notonline",
            },
            alias="onlinenewssource",
            name='Online or not')
        print('Created combination: %s' % onlinenewssource.alias)

        # Combine a single categorical - Combine with subvar 3 on the wrong place
        over35 = dataset.combine_categories(
            agerange,
            map={
                1: [1, 2, 3],
                2: [4, 5]
            },
            categories={
                1: 'under35',
                2: 'over35'
            },
            name='over 35?',
            alias='over35')
        print('Created combination: %s' % over35.alias)

        self.maxDiff = None
        # Export some rows
        output = tempfile.NamedTemporaryFile('rw', delete=True)
        dataset.export(output.name)

        reader = csv.DictReader(output)

        # put the data into columns
        actual = {}
        for row in reader:
            for k, v in row.items():
                actual.setdefault(k, []).append(v)

        reader = csv.reader(StringIO(RECODES_CSV_OUTPUT))
        headers = reader.next()

        # rewrite the actual csv in the same order as the expected csv
        actualf = StringIO()
        writer = csv.writer(actualf)
        writer.writerow(headers)
        for i in range(len(actual.values()[0])):
            writer.writerow([actual[h][i] for h in headers])
        actualf.seek(0)

        result = [l.strip() for l in actualf.read().strip().split('\n')]
        expected = RECODES_CSV_OUTPUT.split('\n')
        # Rows are unordered under streaming conditions
        self.assertEqual(sorted(result), sorted(expected))

        output.close()
        ds.delete()


class TestFill(TestCase):
    def prepare_ds(self):
        cats = [
            {"id": 1, "name": "Daily", "missing": False, "numeric_value": None},
            {"id": 2, "name": "Weekly", "missing": False,
             "numeric_value": None},
            {"id": 3, "name": "Monthly", "missing": False,
             "numeric_value": None},
            {"id": -1, "name": "No Data", "missing": True,
             "numeric_value": None},
        ]
        metadata = {
            "coke_freq": {
                "name": "frequency coke",
                "type": "categorical",
                "categories": cats
            },
            "pepsi_freq": {
                "name": "frequency pepsi",
                "type": "categorical",
                "categories": cats
            },
            "pop_pref": {
                "name": "Soda preference",
                "type": "categorical",
                "categories": [
                    {"id": 1, "name": "Coke", "missing": False,
                     "numeric_value": None},
                    {"id": 2, "name": "Pepsi", "missing": False,
                     "numeric_value": None},
                    {"id": -1, "name": "No Data", "missing": True,
                     "numeric_value": None},
                ]
            }
        }
        ds_payload = {
            'element': 'shoji:entity',
            'body': {
                'name': 'test_fill',
                'table': {
                    'element': 'crunch:table',
                    'metadata': metadata
                },
            }
        }

        rows = [
            ["coke_freq", "pepsi_freq", "pop_pref"],
            [1, 3, 1],
            [2, 2, 1],
            [3, 1, 1],
            [1, 3, 2],
            [2, 2, 2],
            [3, 1, 2],
        ]
        ds = site.datasets.create(ds_payload).refresh()
        dataset = get_mutable_dataset(ds.body.id, site)
        Importer().append_rows(ds, rows)
        return dataset, ds

    def test_fill(self):
        dataset, api_ds = self.prepare_ds()
        dataset.create_fill_values([
            {"case": "pop_pref == 1", "variable": "coke_freq"},
            {"case": "pop_pref == 2", "variable": "pepsi_freq"},
        ], alias="pop_freq", name="Pop frequency")

        variables = api_ds.variables.by("alias")
        new_id = variables["pop_freq"]["id"]
        new_var = variables["pop_freq"].entity
        self.assertTrue(new_var.body.derived)
        self.assertEqual(new_var.body.name, "Pop frequency")

        data = api_ds.follow("table", "limit=6")
        cats = {c["name"]: c["id"] for c in data["metadata"][new_id]["categories"]}
        self.assertEqual(data["data"][new_id], [
            # Coke chunk
            cats["Daily"],
            cats["Weekly"],
            cats["Monthly"],
            # Pepsi chunk
            cats["Monthly"],
            cats["Weekly"],
            cats["Daily"],
        ])
        api_ds.delete()

    def test_fill_w_else(self):
        dataset, ds = self.prepare_ds()
        dataset.create_fill_values([
            {"case": "pop_pref == 1", "variable": "coke_freq"},
            {"case": "else", "variable": "pepsi_freq"},
        ], alias="pop_freq", name="Pop frequency")

        variables = ds.variables.by("alias")
        new_id = variables["pop_freq"]["id"]
        new_var = variables["pop_freq"].entity
        self.assertTrue(new_var.body.derived)
        self.assertEqual(new_var.body.name, "Pop frequency")

        data = ds.follow("table", "limit=6")
        cats = {c["name"]: c["id"] for c in data["metadata"][new_id]["categories"]}
        self.assertEqual(data["data"][new_id], [
            # Coke chunk
            cats["Daily"],
            cats["Weekly"],
            cats["Monthly"],
            # Pepsi chunk - Default case
            cats["Monthly"],
            cats["Weekly"],
            cats["Daily"],
        ])
        ds.delete()

    def test_fill_w_else_code(self):
        dataset, ds = self.prepare_ds()
        dataset.create_fill_values([
            {"case": "pop_pref == 1", "variable": "coke_freq"},
            {"case": "else", "name": "Not Asked", "id": 99, "missing": False},
        ], alias="pop_freq", name="Pop frequency")

        variables = ds.variables.by("alias")
        new_id = variables["pop_freq"]["id"]
        new_var = variables["pop_freq"].entity
        self.assertTrue(new_var.body.derived)
        self.assertEqual(new_var.body.name, "Pop frequency")

        data = ds.follow("table", "limit=6")
        cats = {c["name"]: c["id"] for c in data["metadata"][new_id]["categories"]}
        self.assertEqual(data["data"][new_id], [
            # Coke chunk
            cats["Daily"],
            cats["Weekly"],
            cats["Monthly"],
            # Default value
            cats["Not Asked"],
            cats["Not Asked"],
            cats["Not Asked"],
        ])
        ds.delete()

    def test_fill_w_else_default(self):
        dataset, ds = self.prepare_ds()
        dataset.create_fill_values([
            {"case": "pop_pref == 1", "variable": "coke_freq"},
        ], alias="pop_freq", name="Pop frequency")

        variables = ds.variables.by("alias")
        new_id = variables["pop_freq"]["id"]
        new_var = variables["pop_freq"].entity
        self.assertTrue(new_var.body.derived)
        self.assertEqual(new_var.body.name, "Pop frequency")

        data = ds.follow("table", "limit=6")
        cats = {c["name"]: c["id"] for c in data["metadata"][new_id]["categories"]}
        self.assertEqual(data["data"][new_id], [
            # Coke chunk
            cats["Daily"],
            cats["Weekly"],
            cats["Monthly"],
            # Default value
            {"?": -1},
            {"?": -1},
            {"?": -1},
        ])
        ds.delete()
