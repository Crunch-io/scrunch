# coding: utf-8

"""
This example shows a more complex example using different recodes and combines
to create new variables.
"""

from examples import NEWS_DATASET, NEWS_DATASET_ROWS

from getpass import getpass
from scrunch import connect
from scrunch.datasets import create_dataset

# username = raw_input("Enter email: ")
# password = getpass("Enter password for %s: " % username)
username = 'captain@crunch.io'
password = 'asdfasdf'

#site = connect(username, password, site_url='https://alpha.crunch.io/api/')
site = connect(username, password, site_url='http://localhost:8080/api/')

# Create a dataset for usage

dataset = create_dataset("Recodes example", NEWS_DATASET)
print "Dataset %s created" % dataset.id

# Add data rows
total = dataset.stream_rows(NEWS_DATASET_ROWS)
dataset.push_rows(total)

# Recode a new single response variable

dataset.recode([
    {'id': 1, 'name': 'Underage', 'rules': 'age < 18'},
    {'id': 2, 'name': 'Millenials', 'rules': 'age > 18 and age < 25'},
    {'id': 3, 'name': 'Gen X', 'rules': 'age < 35 and age >= 25'},
    {'id': 4, 'name': 'Grown ups', 'rules': 'age < 60 and age >= 35'},
], alias='agerange', name='Age range', multiple=False)

# Recode a new multiple response variable using the single response as filter
origintype = dataset.recode([
    {'id': 1, 'name': "Online", 'rules': 'newssource.has_any(["newssource-1", "newssource-2", "newssource-3", "newssource-4"])'},
    {'id': 2, 'name': "Print", 'rules': 'newssource.has_any(["newssource-5", "newssource-6"])'},
    {'id': 3, 'name': "Tv", 'rules': 'newssource.has_any(["newssource-7", "newssource-9"])'},
    {'id': 4, 'name': "Radio", 'rules': 'newssource.has_any(["newssource-8", "newssource-10"])'},
], alias='origintype', name="News source by type", multiple=True)

# Add an exclusion filter
dataset.exclude('agerange == 1')  # Remove underage

# MAYBE: Create a new numeric expression based on arbitrary rules

# Copy a variable
origintype_copy = dataset.copy_variable(origintype)

# Export some rows
dataset.download("recodes.csv")
