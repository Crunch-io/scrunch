Advanced Dataset Manipulation
=============================

## Dataset Forks

While editing the dataset you may want to work on a fork. Use the following
helpers to manage the dataset forks.

### Fork the dataset

Starting with the instance of a dataset, use the `.fork()` method to create 
a fork.

```python
ds_fork = ds.fork()
```

The resulting object is also a `scrunch.datasets.Dataset` instance and 
you can use it as with any other dataset instance.

The owner of the fork is the user that created it unless it was owned by a 
Project, in which case the Project ownership will be respected. You can 
change that behavior by adding the `preserve_owner` parameter as `False`, like so:

[comment]: TODO: Check, is this true that preserve_owner=False will avoid the project ownership?

```python
ds_fork = ds.fork(preserve_owner=False)
```


### Delete a fork

The same way you can delete a dataset, the fork can be deleted calling 
the `.delete()` method:

```python
ds_fork.delete()
```

### Delete all forks

It is also possible to delete all forks, just use the `.delete_forks()` 
method from the dataset.

```python
ds.delete_forks()
```

    Note that here we are using the `ds` object, not the 
    `ds_fork`.`

## Savepoints

Savepoints works as snapshots of the current state of the dataset. You 
can create savepoints by doing the following:

```python
ds.create_savepoint('savepoint description')
```

Now if you want to revert any change and reset the dataset to the previous 
savepoint do the following:

```python
ds.load_savepoint('savepoint description')
```

## Advanced usage of Derived variables

scrunch also provides advanced helper methods on `Dataset` objects that map
directly to the `combine_categories` and `combine_responses` expression
functions from the Crunch API.

#### Combine categories

A common operation is to create a new variable out of an existing variable
by combining categories.

Using scrunch's `combine_categories` helper method, we can achieve the
same result as the "recode" example from the intro for the `brandrating` variable
as follows:

```python
category_recode = {
    1: {
        'name': 'Favorable',
        'missing': False,
        'combined_ids': [1, 2]
    },
    2: {
        'name': 'Neutral',
        'missing': False,
        'combined_ids': [3]
    },
    3: {
        'name': 'Unfavorable',
        'missing': False,
        'combined_ids': [4, 5]
    }
}

combined_cats = ds.combine_categories(
    variable='brandrating', 
    category_map=category_recode, 
    name='Brandrating 2', 
    alias='brandrating2', 
    description='Recoding brandrating')
```

#### Combine responses

For a variable with subvariables (like multiple choice questions) you may want
to create a new variable with combined subvariables.

```python
response_mappings = {
    'new_subvar_alias1': ['from_subvar_alias1', 'from_subvar_alias2'],
    'new_subvar_alias2': ['from_subvar_alias3', 'from_subvar_alias4']
}

combined_resps = ds.combine_responses(
    variable='original_variable_alias', 
    response_map=response_mappings,
    name='Brandrating 3', 
    alias='brandrating3', 
    description='Combining responses for brandrating')
```


#### Editing derived variables

It is also possible to edit a previously derived variable. Suppose
we want to remove the Neutral category in the variable create above
`combined_cats`. The function `edit_derived()` will PATCH derived
variables:

```python
category_recode = {
    1: {
        'name': 'Favorable',
        'missing': False,
        'combined_ids': [1, 2]
    },
    2: {
        'name': 'Unfavorable',
        'missing': False,
        'combined_ids': [3, 4, 5]
    }
}
edited = combined_cats.edit_derived(
    variable='brandrating',
    mapper=category_recode
)
```

The method `edit_derived()` is smart enough and can be used to edit
`combine_responses` as well. in the case of the previously created
variable `combined_resps`. We can alter it's mappings with:

```python
response_mappings = {
    'new_subvar_alias1': ['from_subvar_alias1', 'from_subvar_alias2'],
    'new_subvar_alias2': ['from_subvar_alias3', 'from_subvar_alias4'],
    'new_subvar_alias3': ['from_subvar_alias5']
}
edited = combined_resps.edit_derived(
    variable='original_variable_alias'
    mapper=response_mappings
)
```

## Transformations


#### Editing categorical variables

It is also possible to edit the structure or categories of a categorial
variable. The method `edit_categorical` allows us to redefine categories
and rules.

```python
categories = [
    {"id": 1, "name": "Hipsters", "numeric_value": None, "missing": False},
    {"id": 2, "name": "Newbies", "numeric_value": None, "missing": False},
]

rules = ['var1 == 1', 'var1 == 2']

edited = categorical_var.edit_categorical(categories, rules)
```

## Joining and Merging data

Is it possible to join to dataset by a common variable. to do so we make 
use of the `join()` method of a dataset. http://docs.crunch.io/#merging-and-joining-datasets

```python

ds2 = site.datasets.by('name')[0]

ds.join(
    left_var='birthyear', 
    right_ds=ds2, 
    right_var='birthyear', 
    columns=['languagepreference'], 
    filter="languagepreference == 3",
    wait=True)
```

Where `columns` is a list of variables we want to join in the dataset; 
`filter` is an expression for filtering rows in the dataset join operation 
and `wait` indicates the method to wait for Crunch to finish the join or
return a URL with the progress indicator. These last 3 arguments are optional.