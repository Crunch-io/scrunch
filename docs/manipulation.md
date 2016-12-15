Dataset Manipulation
====================

This document describes and gives examples of how
to manipulate the data on datasets.

```python
from pycrunch import get_dataset
ds = get_dataset(dataset='Summer data', site=site)
```

Where `site` is a Crunch API session instance obtained
by `connect()` or `connect_with_token()`.

## Change current editor

Main requirement to manipulate the dataset is to be the current editor. 
Set yourself the current editor by calling:

```python
ds.change_editor(user='me@mail.com')
```

Also is possible to pass the user url:

```python
ds.change_editor(user='https://app.crunch.io/api/users/<userid>')
```

## Dataset Forks

While editing the dataset you may want to work on a fork. Use the following
helpers to manage the dataset forks.

### Fork the dataset

Starting with the instance of a dataset, use the `.fork()` method to create 
a fork.

```python
ds_fork = ds.fork()
```

The resulting object is also a `pycrunch.datasets.Dataset` instance and 
you can use it as with any other dataset instance.

The owner of the fork is the user that created it. If you need to have the
owner of the parent dataset be the owner of the fork, then you need to
set the `preserve_owner` parameter to `True`, like so:

```python
ds_fork = ds.fork(preserve_owner=True)
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

## Drop Rules

#### Exclusion Filters

Drop rules are used to delete invalid cases -- respondents who spent too little 
time answering the survey ("speeders"), cases with inconsistent data, etc. 
In Crunch, these are supported using *exclusion filters*, which are 
specified using a logical expression.

For example, assume that we have `disposition` as the alias of a variable 
in the dataset. Then apply the exclusion filter:

```python
ds.exclude("disposition != 0")
```

(Here, zero is the id (or code) assigned to completed interviews.)

We can also exclude a list of ids using:

```python
ds.exclude("disposition in [0, 1]")
```

Date variables are also supported in exclusion filters, like shown in the
following examples:

```python
ds.exclude('registration_time >= "2015-01-01T00:00:00+00:00"')
ds.exclude('registration_time < "2015-01-01T00:00:00+00:00"')
ds.exclude('registration_time == "2015-01-01T00:00:00+00:00"')
ds.exclude('not(registration_time == "2015-01-01T00:00:00+00:00")')
```

#### Filter expressions

At the moment *filter expressions* can be composed using the following logical expressions:

| operator | meaning               |
|:--------:|-----------------------|
| ==       | equal                 |
| !=       | unequal               |
| >        | greater than          |
| >=       | greater or equal      |
| <        | less than             |
| <=       | less or equal         |
| and      | logical *and*         |
| or       | logical *or*          |
| in       | in *list/tuple*       |
| not in   | not in *list/tuple*   |
| has_any  | has_any(*list/tuple*) |
| has_all  | has_all(*list/tuple*) |

`Note:` The expression needs to contain the **alias** and the **value**.

## Derived variables

### Recodes

Pycrunch's Variable entities provide a `combine` method that tries to mimic
the SPSS recode syntax as much as possible.

For example, if `brandrating` is a variable with categories: `Very favorable`,
`Somewhat favorable`, `Neutral`, `Somewhat unfavorable`, `Very unfavorable`,
`Don't know` (with codes 1,2,3,4,5,9 respectively), we can create a new
combined variable `brandrating2` using the following code:

```python
var = ds.brandrating
new_var = var.combine(
    alias='brandrating2',
    map={1: (1, 2), 2: 3, 3: (4, 5)},
    names=('Favorable', 'Neutral', 'Unfavorable'),
    name='Brandrating 2',
    description='Recoding brandrating'
)
```

Sensible defaults are used whenever possible. For example, we can omit the
`name` and `description` arguments:

```python
var = ds.brandrating
new_var = var.combine(
    alias='brandrating2',
    map={1: (1, 2), 2: 3, 3: (4, 5)},
    names=('Favorable', 'Neutral', 'Unfavorable')
)
```

in which case the name of the new variable would be the original name plus the
" (recoded)" suffix and the description would be same as the original variable.

The `combine` method also works on `multiple_response` Variables. For example:

```python
var = ds.mult_resp
new_var = var.combine(
    alias='recoded_mult_resp',
    map={
        'new_subvar_alias1': ('orig_subvar_alias1', 'orig_subvar_alias2'),
        'new_subvar_alias2': ('orig_subvar_alias3', 'orig_subvar_alias4')
    },
    name='Re-coded multiple response',
    description='Combining responses for mult_resp'
)
```

Note: the SPSS-like `recode` method only works on `categorical`,
`categorical_array` and `multiple_response` Variable entities.

### Advanced usage

Pycrunch also provides advanced helper methods on `Dataset` entities that map
directly to the `combine_categories` and `combine_responses` expression
functions from the Crunch API.

#### Combine categories

A common operation is to create a new variable out of an existing variable
by combining categories.

Using pycrunch's `combine_categories` helper method, we can achieve the
same result as the previous "combine" example for the `brandrating` variable
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

#### Creating a categorical variable

Transformations create new variables based upon the values of one or more input variables. 

```python
categories = [
    {"id": 1, "name": "Hipsters", "numeric_value": None, "missing": False},
    {"id": 2, "name": "Techies", "numeric_value": None, "missing": False},
    {"id": 3, "name": "Others", "numeric_value": None, "missing": False}
]

rules = ['var1 == 1 and var2 == 1', 'var1 == 2 and var2 == 2']

categorical_var = ds.create_categorical(
    categories=categories,
    rules=rules,
    name='New variable',
    alias='alias', 
    description='description')
```

*Rules* are evaluated in order (as if this were a sequence of if/elif/else statements. The values for the rule expressions must be valid values on the source variables.

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