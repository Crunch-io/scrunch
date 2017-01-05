# coding: utf-8

"""
This example shows a more complex example using different recodes and combines
to create new variables.
"""

from examples import NEWS_DATASET, NEWS_DATASET_ROWS, mr_in

from getpass import getpass
from scrunch import connect
from scrunch.datasets import create_dataset

HOST = 'https://alpha.crunch.io'

username = raw_input("Enter email: ")
password = getpass("Enter password for %s: " % username)

site = connect(username, password, site_url='%s/api/' % HOST)

# Create a dataset for usage
dataset = create_dataset("Recodes example", NEWS_DATASET)
print("Dataset %s created" % dataset.id)

# Add data rows
total = dataset.stream_rows(NEWS_DATASET_ROWS)
dataset.push_rows(total)

# Recode a new single response variable
agerange = dataset.create_categorical([
    {'id': 1, 'name': 'Underage', 'rules': 'age < 18'},
    {'id': 2, 'name': 'Millenials', 'rules': 'age > 18 and age < 25'},
    {'id': 3, 'name': 'Gen X', 'rules': 'age < 35 and age >= 25'},
    {'id': 4, 'name': 'Grown ups', 'rules': 'age < 60 and age >= 35'},
    {'id': 5, 'name': '60+', 'rules': 'age >= 60'}
], alias='agerange', name='Age range', multiple=False)

print("Variable %s created" % agerange.alias)

# Recode a new multiple response variable from an existing multiple response variable
origintype = dataset.create_categorical([
    {'id': 1, 'name': "Online",
     # Mixed support for using "category"(subvariables really) IDs
     'rules': mr_in(dataset, 'newssource', [1, 2, 3, 4])},  # Only in the helper
    {'id': 2, 'name': "Print", 'rules': mr_in(dataset, 'newssource', [5, 6])},
    {'id': 3, 'name': "Tv", 'rules': mr_in(dataset, 'newssource', [7, 9])},
    {'id': 4, 'name': "Radio", 'rules': mr_in(dataset, 'newssource', [8, 10])},
], alias='origintype', name="News source by type", multiple=True)

print("Variable %s created" % origintype.alias)

# Add an exclusion filter
dataset.exclude('agerange == 1')  # Remove underage

# MAYBE: Create a new numeric expression based on arbitrary rules

# Copy a variable
origintype_copy = dataset.copy_variable(origintype, name='Copy of origintype',
    alias='origintype_copy')
print("Variable %s created" % origintype_copy.alias)


onlinenewssource = dataset.combine(origintype_copy, [
    {"id": 1, "name": 'online', 'combined_ids': [1]},
    {"id": 2, "name": 'notonline', 'combined_ids': [2, 3, 4]}
], name='Online or not', alias='onlinenewssource')
print('Created combination: %s' % onlinenewssource.alias)

# Combine a single categorical - Combine with subvar 3 on the wrong place
over35 = dataset.combine(agerange, [
    {"id": 1, "name": 'under35', 'combined_ids': [1, 2, 3], 'missing': False},
    {"id": 2, "name": 'over35', 'combined_ids': [4, 5], 'missing': False}
], name='over 35?', alias='over35')
print('Created combination: %s' % over35.alias)

# Export some rows
dataset.download("recodes.csv")

print('Visit on the web: %s' % dataset.web_url(HOST))
