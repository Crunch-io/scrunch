#!/usr/bin/env python

import math
import os

import isodate
import pycrunch
import pytest

pytest.mark.skip('skip test discovery on this module')

try:
    from pycrunch import pandaslib
except ImportError:
    # pandas is not installed
    pandaslib = None

from scrunch import connect
from scrunch.datasets import Variable, get_geodata
from scrunch.streaming_dataset import StreamingDataset


CRUNCH_URL = os.environ.get('CRUNCH_TEST_URL')
CRUNCH_USER = os.environ.get('CRUNCH_TEST_USER')
CRUNCH_PASSWORD = os.environ.get('CRUNCH_TEST_PASSWORD')

# Metadata.
DATASET_DOC = {
    'body': {
        'name': 'scrunch test dataset',
        'description': 'scrunch integration tests',
        'table': {
            'element': 'crunch:table',
            'metadata': {
                'identity': {
                    'alias': 'identity',
                    'name': 'ID',
                    'type': 'numeric'
                },
                'ip_address': {
                    'alias': 'ip_address',
                    'name': 'Public IP Address',
                    'type': 'text'
                },
                'operating_system': {
                    'alias': 'operating_system',
                    'name': 'Operating System',
                    'type': 'text'
                },
                'registration_time': {
                    'alias': 'registration_time',
                    'name': 'Registration Time',
                    'resolution': 'ms',
                    'type': 'datetime'
                },
                'speak_spanish': {
                    'alias': 'speak_spanish',
                    'categories': [
                        {
                            'id': 1,
                            'missing': False,
                            'name': 'I speak Spanish primarily',
                            'numeric_value': 1
                        },
                        {
                            'id': 2,
                            'missing': False,
                            'name': 'I speak both Spanish and English equally',
                            'numeric_value': 2
                        },
                        {
                            'id': 3,
                            'missing': False,
                            'name': 'I speak English primarily but can speak Spanish',
                            'numeric_value': 3
                        },
                        {
                            'id': 4,
                            'missing': False,
                            'name': 'I can not speak Spanish',
                            'numeric_value': 4
                        },
                        {
                            'id': 32766,
                            'missing': True,
                            'name': 'skipped',
                            'numeric_value': None
                        },
                        {
                            'id': 32767,
                            'missing': True,
                            'name': 'not asked',
                            'numeric_value': None
                        },
                        {
                            'id': -1,
                            'missing': True,
                            'name': 'No Data',
                            'numeric_value': None
                        }
                    ],
                    'name': 'Do you speak Spanish?',
                    'type': 'categorical'
                },
                'hobbies': {
                    'alias': 'hobbies',
                    'categories': [
                        {
                            'id': 1,
                            'missing': False,
                            'name': 'Very interested',
                            'numeric_value': 1
                        },
                        {
                            'id': 2,
                            'missing': False,
                            'name': 'Somewhat interested',
                            'numeric_value': 2
                        },
                        {
                            'id': 3,
                            'missing': False,
                            'name': 'A little interested',
                            'numeric_value': 3
                        },
                        {
                            'id': 4,
                            'missing': False,
                            'name': 'Not at all interested',
                            'numeric_value': 4
                        },
                        {
                            'id': 32766,
                            'missing': True,
                            'name': 'skipped',
                            'numeric_value': None
                        },
                        {
                            'id': 32767,
                            'missing': True,
                            'name': 'not asked',
                            'numeric_value': None
                        },
                        {
                            'id': -1,
                            'missing': True,
                            'name': 'No Data',
                            'numeric_value': None
                        }
                    ],
                    'name': 'Hobbies',
                    'subvariables': [
                        {
                            'alias': 'hobbies_1',
                            'name': 'Sports'
                        },
                        {
                            'alias': 'hobbies_2',
                            'name': 'Video Games'
                        },
                        {
                            'alias': 'hobbies_3',
                            'name': 'Reading'
                        },
                        {
                            'alias': 'hobbies_4',
                            'name': 'Outdoor Activities'
                        }
                    ],
                    'type': 'categorical_array'
                },
                'music': {
                    'alias': 'music',
                    'categories': [
                        {
                            'id': 1,
                            'missing': False,
                            'name': 'selected',
                            'numeric_value': 1,
                            'selected': True
                        },
                        {
                            'id': 2,
                            'missing': False,
                            'name': 'not selected',
                            'numeric_value': 2,
                            'selected': False
                        },
                        {
                            'id': 32767,
                            'missing': True,
                            'name': 'not asked',
                            'numeric_value': None
                        },
                        {
                            'id': 32766,
                            'missing': True,
                            'name': 'skipped',
                            'numeric_value': None
                        },
                        {
                            'id': -1,
                            'missing': True,
                            'name': 'No Data',
                            'numeric_value': None
                        }
                    ],
                    'name': 'Music',
                    'subvariables': [
                        {
                            'alias': 'music_1',
                            'name': 'Pop'
                        },
                        {
                            'alias': 'music_2',
                            'name': 'Rock'
                        },
                        {
                            'alias': 'music_97',
                            'name': 'Other'
                        },
                        {
                            'alias': 'music_98',
                            'name': 'Don\'t know'
                        },
                        {
                            'alias': 'music_99',
                            'name': 'None of these'
                        }
                    ],
                    'type': 'multiple_response'
                },
                'religion': {
                    'alias': 'religion',
                    'categories': [
                        {
                            'id': 1,
                            'missing': False,
                            'name': 'Protestant',
                            'numeric_value': 1
                        },
                        {
                            'id': 2,
                            'missing': False,
                            'name': 'Catholic',
                            'numeric_value': 2
                        },
                        {
                            'id': 3,
                            'missing': False,
                            'name': 'Jewish',
                            'numeric_value': 3
                        },
                        {
                            'id': 4,
                            'missing': False,
                            'name': 'Muslim',
                            'numeric_value': 4
                        },
                        {
                            'id': 5,
                            'missing': False,
                            'name': 'None',
                            'numeric_value': 5
                        },
                        {
                            'id': 6,
                            'missing': False,
                            'name': 'Other',
                            'numeric_value': 6
                        },
                        {
                            'id': 32766,
                            'missing': True,
                            'name': 'skipped',
                            'numeric_value': None
                        },
                        {
                            'id': 32767,
                            'missing': True,
                            'name': 'not asked',
                            'numeric_value': None
                        },
                        {
                            'id': -1,
                            'missing': True,
                            'name': 'No Data',
                            'numeric_value': None
                        }
                    ],
                    'name': 'What is your religious preference?',
                    'type': 'categorical'
                },
                'location': {
                    'alias': 'location',
                    'categories': [
                        {
                            'id': 1,
                            'missing': False,
                            'name': 'Protestant',
                            'numeric_value': 1
                        }, {
                            'id': 2,
                            'missing': False,
                            'name': 'North East',
                            'numeric_value': 2
                        }, {
                            'id': 3,
                            'missing': False,
                            'name': 'North West',
                            'numeric_value': 3
                        }, {
                            'id': 4,
                            'missing': False,
                            'name': 'Yorkshire and the Humber',
                            'numeric_value': 4
                        }, {
                            'id': 5,
                            'missing': False,
                            'name': 'East Midlands',
                            'numeric_value': 5
                        }, {
                            'id': 6,
                            'missing': False,
                            'name': 'West Midlands',
                            'numeric_value': 6
                        }, {
                            'id': 7,
                            'missing': False,
                            'name': 'East of England',
                            'numeric_value': 7
                        }, {
                            'id': 8,
                            'missing': False,
                            'name': 'London',
                            'numeric_value': 8
                        }, {
                            'id': 9,
                            'missing': False,
                            'name': 'South East',
                            'numeric_value': 9
                        }, {
                            'id': 10,
                            'missing': False,
                            'name': 'South West',
                            'numeric_value': 10
                        }, {
                            'id': 11,
                            'missing': False,
                            'name': 'Wales',
                            'numeric_value': 11
                        }, {
                            'id': 12,
                            'missing': False,
                            'name': 'Scotland',
                            'numeric_value': 12
                        }, {
                            'id': 13,
                            'missing': False,
                            'name': 'Northern Ireland',
                            'numeric_value': 13
                        },
                    ],
                    'name': 'Where do you live?',
                    'type': 'categorical'
                }
            },
            'order': [
                {
                    'entities': [
                        'identity',
                        'ip_address',
                        'operating_system',
                        'registration_time',
                        'speak_spanish',
                        'hobbies',
                        'music',
                        'religion',
                        'location'
                    ],
                    'group': 'ungrouped'
                }
            ]
        }
    }
}

# Data
ROWS = [
    ['identity', 'ip_address', 'operating_system', 'registration_time', 'speak_spanish', 'hobbies_1', 'hobbies_2', 'hobbies_3', 'hobbies_4', 'music_1', 'music_2', 'music_97', 'music_98', 'music_99', 'religion', 'location'],
    [1, '10.0.0.1', 'Linux', '2014-04-21T10:00:00+00:00', 1, 32767, 32767, 32767, 32767, 2, 2, 1, 2, 2, 1, 1],
    [2, '10.0.0.2', 'Solaris', '2014-05-10T00:00:00+00:00', 1, 32766, 1, 1, 4, 1, 1, 1, 2, 2, 2, 2],
    [3, '10.0.0.3', 'Linux', '2015-01-01T00:00:00+00:00', 2, 2, 1, 2, 2, 2, 2, 2, 2, 1, 3, 3],
    [4, '10.0.0.4', 'Windows', '2015-01-02T00:00:00+00:00', 3, 4, 3, 2, 1, 1, 2, 1, 2, 2, 4, 4],
    [5, '10.0.0.5', 'Windows', '2015-02-01T00:00:00+00:00', 1, 1, 2, 32766, 4, 1, 1, 1, 2, 2, 5, 5],
    [6, '10.0.0.6', 'MacOS', '2015-06-01T00:00:00+00:00', 4, 2, 4, 4, 1, 2, 2, 1, 2, 2, 6, 6],
    [7, '10.0.0.7', 'Windows', '2015-12-30T00:00:00+00:00', 32766, 1, 32766, 4, 3, 2, 2, 2, 1, 2, 32766, 7],
    [8, '10.0.0.8', 'Minix', '2016-01-01T00:00:00+00:00', 32766, 2, 1, 1, 2, 2, 2, 2, 1, 2, 32767, 8],
    [9, '10.0.0.9', 'FreeBSD', '2016-02-01T00:00:00+00:00', 32767, 1, 1, 1, 32766, 1, 2, 1, 2, 2, 1, 9],
    [10, '10.0.0.10', 'NetBSD', '2015-03-01T00:00:00+00:00', 2, 4, 3, 4, 1, 2, 2, 1, 2, 2, 2, 10],
    [11, '10.0.0.10', 'NetBSD', '2015-03-01T00:01:00+00:00', 2, 4, 3, 4, 1, 1, 1, 1, 1, 1, 3, 11],
    [12, '10.0.0.10', 'NetBSD', '2015-03-01T00:02:00+00:00', 2, 4, 3, 4, 1, 2, 2, 2, 2, 2, 4, 12],
]


def invalid_credentials():
    return any(
        item is None
        for item in (CRUNCH_URL, CRUNCH_USER, CRUNCH_PASSWORD)
    )


def isnan(obj):
    if not isinstance(obj, float):
        return False
    return math.isnan(obj)


def main():
    assert not invalid_credentials()
    assert pandaslib, 'Pandas library not installed'

    # Login.
    site = connect(CRUNCH_USER, CRUNCH_PASSWORD, CRUNCH_URL)
    assert isinstance(site, pycrunch.shoji.Catalog)

    # Create the test dataset.
    dataset_resource = site.datasets.create(DATASET_DOC).refresh()
    assert isinstance(dataset_resource, pycrunch.shoji.Entity)
    dataset = StreamingDataset(dataset_resource)

    try:
        # Load initial data.
        pycrunch.importing.importer.append_rows(dataset.resource, ROWS)

        # refresh dataset instance, so size is updated
        dataset.resource.refresh()

        # Check the initial number of rows.
        df = pandaslib.dataframe(dataset.resource)
        assert len(df) == len(ROWS) - 1  # excluding the header
        assert dataset.size.rows == len(df)
        assert dataset.size.unfiltered_rows == len(df)

        # Also check number of columns
        columns = DATASET_DOC['body']['table']['metadata'].__len__()
        assert dataset.size.columns == columns

        # 0. Manipulate metadata

        # 0.1 Start by updating the missing rules for the `identity` variable
        identity_missing_rules = {
            "not asked": 9999,
            "skipped": 9998
        }

        assert dataset['identity'].missing_rules == {}
        dataset['identity'].set_missing_rules(identity_missing_rules)
        assert dataset['identity'].missing_rules == identity_missing_rules

        # 0.2 Try setting and unsetting the geodata view
        location = dataset['location']
        geodata = get_geodata('UK Regions')
        assert 'geodata' not in location.view

        # Set geodata using Entity object
        location.set_geodata_view(geodata, feature_key='EER13NM')
        assert 'geodata' in location.view

        location.unset_geodata_view()
        assert 'geodata' not in location.view

        # Set geodata using url
        location.set_geodata_view(geodata.self, feature_key='EER13NM')
        assert 'geodata' in location.view

        location.unset_geodata_view()
        assert 'geodata' not in location.view

        # Set geodata using name
        location.set_geodata_view('UK Regions', feature_key='EER13NM')
        assert 'geodata' in location.view

        location.unset_geodata_view()
        assert 'geodata' not in location.view

        # 1. Exclusion Filter Integration Tests

        # 1.1 Set a simple exclusion filter.

        dataset.exclude('identity > 5')
        df = pandaslib.dataframe(dataset.resource)
        assert len(df) == 5
        assert not any(r['identity'] > 5 for _, r in df.iterrows())

        # 1.2 More complex exclusion filters involving a categorical variable.

        expr = 'speak_spanish in [32766]'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] != 32766
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids

        expr = 'speak_spanish in (32766, 32767)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] not in (32766, 32767)
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert not isnan(row['speak_spanish'])

        expr = 'not (speak_spanish in (1, 2) and operating_system == "Linux")'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] in (1, 2) and row[2] == 'Linux'
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['speak_spanish'] in \
                ('I speak Spanish primarily',
                    'I speak both Spanish and English equally')
            assert row['operating_system'] == 'Linux'

        # 1.3 Exclusion filters with `any`.

        expr = 'hobbies.any([32766])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and 32766 not in row[5:9]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert {'?': 32766} not in row['hobbies']

        expr = 'not hobbies.any([32766])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and 32766 in row[5:9]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert {'?': 32766} in row['hobbies']

        expr = 'hobbies.any([32766, 32767])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity'
            and 32766 not in row[5:9] and 32767 not in row[5:9]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert {'?': 32766} not in row['hobbies'] and \
                   {'?': 32767} not in row['hobbies']

        expr = 'music.any([32766])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and 32766 not in row[9:14]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert {'?': 32766} not in row['music']

        expr = 'music.any([1])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and 1 not in row[9:14]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert 1 not in row['music']

        expr = 'music.any([1, 2])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity'
            and 1 not in row[9:14] and 2 not in row[9:14]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert 1 not in row['music'] and 2 not in row['music']

        # 1.4 Exclusion filters with `all`.

        expr = 'hobbies.all([32767])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[5:9] != [32767, 32767, 32767, 32767]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['hobbies'] != [{'?': 32767}, {'?': 32767},
                                      {'?': 32767}, {'?': 32767}]

        expr = 'not hobbies.all([32767])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[5:9] == [32767, 32767, 32767, 32767]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['hobbies'] == [{'?': 32767}, {'?': 32767},
                                      {'?': 32767}, {'?': 32767}]

        expr = 'music.all([1])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[9:14] != [1, 1, 1, 1, 1]
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['music'] != [1, 1, 1, 1, 1]

        expr = 'music.all([1]) or music.all([2])'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity'
            and (row[9:14] != [1, 1, 1, 1, 1] and row[9:14] != [2, 2, 2, 2, 2])
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['music'] != [1, 1, 1, 1, 1] and \
                row['music'] != [2, 2, 2, 2, 2]

        expr = 'not ( music.all([1]) or music.all([2]) )'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity'
            and (row[9:14] == [1, 1, 1, 1, 1] or row[9:14] == [2, 2, 2, 2, 2])
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['music'] == [1, 1, 1, 1, 1] or \
                row['music'] == [2, 2, 2, 2, 2]

        # 1.5 Exclusion filters with `duplicates`.

        expr = 'ip_address.duplicates()'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        seen_ip_addresses = []
        for _, row in df.iterrows():
            assert row['ip_address'] not in seen_ip_addresses
            seen_ip_addresses.append(row['ip_address'])

        # 1.6 Exclusion filters with `valid` and `missing`.

        expr = 'valid(speak_spanish)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] in (32766, 32767)
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert isnan(row['speak_spanish'])

        expr = 'not valid(speak_spanish)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] not in (32766, 32767)
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert not isnan(row['speak_spanish'])

        expr = 'missing(speak_spanish)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] not in (32766, 32767)
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert not isnan(row['speak_spanish'])

        expr = 'missing(hobbies)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity'
            and (row[5:9] != [32766, 32766, 32766, 32766]
                 and row[5:9] != [32767, 32767, 32767, 32767])
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['hobbies'] != [{'?': 32766}, {'?': 32766},
                                      {'?': 32766}, {'?': 32766}] \
                and row['hobbies'] != [{'?': 32767}, {'?': 32767},
                                       {'?': 32767}, {'?': 32767}]

        expr = 'not missing(hobbies)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity'
            and (row[5:9] == [32766, 32766, 32766, 32766]
                 or row[5:9] == [32767, 32767, 32767, 32767])
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['hobbies'] == [{'?': 32766}, {'?': 32766},
                                      {'?': 32766}, {'?': 32766}] \
                or row['hobbies'] == [{'?': 32767}, {'?': 32767},
                                      {'?': 32767}, {'?': 32767}]

        expr = 'valid(hobbies)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and (32766 in row[5:9] or 32767 in row[5:9])
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert {'?': 32766} in row['hobbies'] or \
                   {'?': 32767} in row['hobbies']

        expr = 'not valid(hobbies)'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity'
            and (32766 not in row[5:9] and 32767 not in row[5:9])
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert {'?': 32766} not in row['hobbies'] and \
                   {'?': 32767} not in row['hobbies']

        # 1.7 Exclusion filter that refers to a subvariable by alias.
        expr = 'hobbies_1 == 4'
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and row[5] != 4
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids
            assert row['hobbies'][0] != 4

        # 1.8 Complex exclusion filters (multiple rules)
        expr = (
            '(religion != 1 and (not valid(speak_spanish) or speak_spanish >= 1)) '
            'or (religion == 1 and speak_spanish == 2) '
            'or (religion == 3 and speak_spanish == 4)'
        )
        dataset.exclude(expr)

        # 1.9 Exclusion filters using date variables.
        dt_str = '2014-12-30T00:00:00+00:00'
        dt = isodate.parse_datetime(dt_str)
        expr = 'registration_time < "%s"' % dt_str
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and isodate.parse_datetime(row[3]) >= dt
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids

        dt_str = '2015-01-01T00:00:00+00:00'
        dt = isodate.parse_datetime(dt_str)
        expr = 'registration_time >= "%s"' % dt_str
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and isodate.parse_datetime(row[3]) < dt
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids

        dt_str = '2014-05-10T00:00:00+00:00'
        dt = isodate.parse_datetime(dt_str)
        expr = 'registration_time == "%s"' % dt_str
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and isodate.parse_datetime(row[3]) != dt
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids

        dt_str = '2014-05-10T00:00:00+00:00'
        dt = isodate.parse_datetime(dt_str)
        expr = 'not(registration_time == "%s")' % dt_str
        dataset.exclude(expr)
        df = pandaslib.dataframe(dataset.resource)
        valid_ids = [
            row[0] for row in ROWS
            if row[0] != 'identity' and isodate.parse_datetime(row[3]) == dt
        ]
        assert len(df) == len(valid_ids)
        for _, row in df.iterrows():
            assert row['identity'] in valid_ids

        # 1.10 Clear the exclusion filter.
        dataset.exclude()
        df = pandaslib.dataframe(dataset.resource)
        assert len(df) == len(ROWS) - 1  # excluding the header

        # 2. Integration Tests for "Transformations".

        categories = [
            {
                'id': 1,
                'name': 'Nerds',
                'numeric_value': 1,
                'missing': False,
                'case': 'operating_system in ("Linux", "Solaris", "Minix", "FreeBSD", "NetBSD")',
            },
            {
                'id': 2,
                'name': 'Normal Users',
                'numeric_value': 2,
                'missing': False,
                'case': 'operating_system == "Windows"',
            },
            {
                'id': 3,
                'name': 'Hipsters',
                'numeric_value': 3,
                'missing': False,
                'case': 'operating_system == "MacOS"',
            },
            {
                'id': 32767,
                'name': 'Unknown',
                'numeric_value': None,
                'missing': True,
                'case': 'missing(operating_system)'
            }
        ]

        new_var = dataset.create_single_response(
            categories=categories,
            name='Operating System Users',
            alias='operating_system_users',
            description='Type of Operating System Users'
        )

        assert isinstance(new_var, Variable)
        assert new_var.type == 'categorical'

        # Check the data on the new variable.
        df = pandaslib.dataframe(dataset.resource)
        assert 'operating_system_users' in df

        # Check the nerds.
        assert set(
            r['operating_system']
            for _, r in df[df['operating_system_users'] == 'Nerds'].iterrows()
        ) == {'Linux', 'Solaris', 'Minix', 'FreeBSD', 'NetBSD'}

        # Check the hipsters.
        assert set(
            r['operating_system']
            for _, r in df[df['operating_system_users'] == 'Hipsters'].iterrows()
        ) == {'MacOS'}

        # Check normal users.
        assert set(
            r['operating_system']
            for _, r in df[df['operating_system_users'] == 'Normal Users'].iterrows()
        ) == {'Windows'}

        # 3. Integration Tests for "Recodes".

        # 3.1 combine_categories.

        # On a 'categorical' variable.
        cat_map = {
            1: [2, 3],
            2: [1, 4],
            99: [32766, 32767]
        }

        cat_names = {
            1: 'Bilingual',
            2: 'Not Bilingual',
            99: 'Unknown'
        }

        new_var = dataset.combine_categorical(
            'speak_spanish',
            map=cat_map,
            categories=cat_names,
            name='Bilingual Person',
            alias='bilingual',
            missing=[99]
        )

        assert isinstance(new_var, Variable)
        assert new_var.type == 'categorical'

        df = pandaslib.dataframe(dataset.resource)
        assert 'bilingual' in df

        # Check the data in the recoded variable.
        bilingual_ids = set(
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] in (2, 3)
        )
        assert set(
            int(r['identity'])
            for _, r in df[df['bilingual'] == 'Bilingual'].iterrows()
        ) == bilingual_ids

        non_bilingual_ids = set(
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] in (1, 4)
        )
        assert set(
            int(r['identity'])
            for _, r in df[df['bilingual'] == 'Not Bilingual'].iterrows()
        ) == non_bilingual_ids

        bilingual_null_ids = set(
            row[0] for row in ROWS
            if row[0] != 'identity' and row[4] in (32766, 32767)
        )
        assert set(
            int(r['identity'])
            for _, r in df[df['bilingual'].isnull()].iterrows()
        ) == bilingual_null_ids

        # On a 'categorical_array' variable.

        cat_map = {
            1: [1, 2],
            2: [3, 4],
            99: [32766, 32767]
        }

        cat_names = {
            1: 'Interested',
            2: 'Not interested',
            99: 'Unknown',
        }

        new_var = dataset.combine_categorical(
            'hobbies',
            map=cat_map,
            categories=cat_names,
            name='Hobbies (recoded)',
            alias='hobbies_recoded',
            missing=[99]
        )
        assert isinstance(new_var, Variable)
        assert new_var.type == 'categorical_array'

        df = pandaslib.dataframe(dataset.resource)
        assert 'hobbies_recoded' in df

        # Check the data in the recoded variable.
        for _, row in df.iterrows():
            hobbies = row['hobbies']
            hobbies_rec = row['hobbies_recoded']
            assert len(hobbies) == len(hobbies_rec)

            for i, value in enumerate(hobbies):
                if value in ({'?': 32766}, {'?': 32767}):
                    assert hobbies_rec[i] == {'?': 99}
                elif value in (1, 2):
                    assert hobbies_rec[i] == 1
                elif value in (3, 4):
                    assert hobbies_rec[i] == 2

        # 3.2 combine_responses.

        cat_map = {
            1: [1, 2],
            2: [97],
            3: [98, 99]
        }

        cat_names = {
            1: 'music_recoded_1',
            2: 'music_recoded_2',
            3: 'music_recoded_3'
        }
        new_var = dataset.combine_multiple_response(
            'music',
            map=cat_map,
            categories=cat_names,
            name='Music (alt)',
            alias='music_recoded'
        )
        assert isinstance(new_var, Variable)
        assert new_var.type == 'multiple_response'

        df = pandaslib.dataframe(dataset.resource)
        assert 'music_recoded' in df

        # TODO: Test the data in the recoded variable. Unsure of its meaning.

    finally:
        dataset.resource.delete()


if __name__ == '__main__':
    main()
    exit(0)
