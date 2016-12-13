# coding: utf-8

from getpass import getpass
from scrunch import connect
from scrunch.datasets import get_dataset, create_dataset

username = raw_input("Enter email: ")
password = getpass("Enter password for %s: " % username)

site = connect(username, password, site_url='https://alpha.crunch.io/api/')

new_ds = create_dataset('Test dataset', {
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

print "Dataset %s created" % new_ds.id

rows = {
    'catvar': [1, 2, 3, 1, 2],
    'numvar': [-1, 0, 1, 2, 3],
    'textvar': ['a', 'b', 'c', 'd', 'e'],
    'cavar': [[1, 2], [1, 1], [3, 2], [2, 2], [3, 1]]
}

new_ds.rename('My Dataset')

dataset = get_dataset(new_ds.id)

# Assert it is the same dataset
assert dataset.id == new_ds.id

total = dataset.stream_rows(rows)
dataset.push_rows(total)

print "Added %s rows" % len(rows.values()[0])

dataset.exclude("numvar > 0")

print "Exclusion filter set"

dataset.create_savepoint("After setting exclusion")

print "Created savepoint"

catvar = dataset['catvar']

combined = dataset.combine_categories(catvar, {
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

assert combined.resource.body.derivation['function'] == 'combine_categories'
print "Variable combined as: %s" % combined.alias

numvar = dataset['numvar']
copy_numeric = dataset.copy_variable(numvar, name='Copied numvar',
                                     alias='copied_numvar')

assert copy_numeric.resource.body.derivation['function'] == 'copy_variable'

dataset.download('rows.csv')#,
                 #variables=[catvar, copy_numeric],
                 #filter='combined == 1')




