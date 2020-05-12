# coding: utf-8

"""
This example shows a more complex example using different recodes and combines
to create new variables.
"""

import os
import tempfile
from unittest import TestCase

from fixtures import NEWS_DATASET, NEWS_DATASET_ROWS, mr_in, RECODES_CSV_OUTPUT
from scrunch import connect
from scrunch.streaming_dataset import get_streaming_dataset

HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']


site = connect(username, password, HOST)


class TestRecodes(TestCase):
    def test_recodes(self):
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
        result = [l.strip() for l in output.read().strip().split('\n')]
        expected = RECODES_CSV_OUTPUT.split('\n')
        # Rows are unordered under streaming conditions
        self.assertEqual(sorted(result), sorted(expected))
        output.close()
        ds.delete()
