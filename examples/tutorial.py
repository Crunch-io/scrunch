# coding: utf-8

from scrunch import connect
from scrunch.datasets import get_dataset, create_dataset

username = ''
password = ''

site = connect(username, password)

new_ds = create_dataset(site, 'test dataset', {
    "catvar": {
        'name': 'categorical variable',
        'type': 'categorical',
        'categories': [{
            'id': 1,
            'name': 'yes',
            'numeric_value': 1,
            'missing': False
        }, {
            'id': 2,
            'name': 'no',
            'numeric_value': 2,
            'missing': False
        }, {
            'id': 3,
            'name': 'Not asked',
            'numeric_value': None,
            'missing': True
        }]
    },
    "numvar": {
        'name': 'numeric variable',
        'type': 'numeric',
    },
    "textvar": {
        'name': 'text variable',
        'type': 'text',
    },
    'cavar': {
        'name': 'categorical array variable',
        'type': 'categorical_array',
        'discarded': True,
        'categories': [{
            'id': 1,
            'name': 'Not likely',
            'numeric_value': -1,
            'missing': False
        }, {
            'id': 2,
            'name': 'Likely',
            'numeric_value': 0,
            'missing': False
        }, {
            'id': 3,
            'name': 'Very likely',
            'numeric_value': 1,
            'missing': False
        }],
        'subreferences': [{
            'name': 'Will do A',
        }, {
            'name': 'Will do B',
        }]
    }
})

rows = {
    'catvar': [1, 2, 3, 1, 2],
    'numvar': [-1, 0, 1, 2, 3],
    'textvar': ['a', 'b', 'c', 'd', 'e'],
    'cavar': [[1, 2], [1, 1], [3, 2], [2, 2], [3, 1]]
}

new_ds.rename("My dataset")

dataset = get_dataset(site, "My dataset")

dataset.add_rows(rows)

dataset.exclude("numvar > 0")

dataset.make_savepoint("After setting exclusion")

catvar = dataset.variables.catvar

combined = catvar.combine_categories({
    1: {
        'name': 'valid',
        'missing': False,
        'combined_ids': [1, 2]
    },
    2: {
        'name': 'invalid',
        'missing': True,
        'combined_ids': [3]
    }
}, name='combined', alias='combined')

copy_numeric = dataset.variables.numvar.copy()

dataset.download('/home/user/rows.csv',
                 variables=[catvar, copy_numeric],
                 filter='combined == 1')




