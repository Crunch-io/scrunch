Examples
--------

#### Creating a Dataset

You need to provide a dictionary containing the basic body for the dataset.

```python
doc = {
    'body': {
        'name': 'Dataset 1'
     }
}

ds = site.datasets.create(doc)
```

#### Loading an existing Dataset

Here's how you can load an existing dataset.

```python
from pycrunch import get_dataset
ds = get_dataset('Dataset 1')
```

#### Creating a variable

In this example we create a categorical variable. As with the dataset, you
have to provide a dictionary with a body element.

```python
var_body = {
    'body': {
        'name': 'Gender',
        'alias': 'gender',
        'type': 'categorical',
        'categories': [
            {'id': 1, 'name': 'M', 'numeric_value': None, 'missing': False},
            {'id': 2, 'name': 'F', 'numeric_value': None, 'missing': False},
            {'id': -1, 'name': 'No Data', 'numeric_value': None, 'missing': True}
        ],
        'values': [1, 2, 2, 2, 2, 1, {'?': -1}]
    }
}

var = ds.variables.create(var_body)
```

#### Loading an existing variable

The common way to load an existing variable entity is by accessing the 
`variables` catalog, then search the variable by its alias, like the 
code below:

```python
var = ds.variables.by('alias').get('gender').entity
```

Variables are also available as attributes in the dataset entity:

```python
var = ds.gender
```

#### Change the variable's attributes

You can change any of the variable's attributes by providing them as keyword
arguments to the *edit* method:

```python
var.edit(name='my new name', alias='gender_ng')
```

#### Adding a description

```python
var.edit(description='My awesome description')
```

#### Hiding variables in the UI

`Note` that hidden variables are simply not shown in the UI but still
accessible through the API!

```python
var.edit(discarded=True)
```

#### Changing categories

Either provide a complete list of new categories like in the *gender* 
example above or if you want to change only the name of a category 
you can achieve that with:

```python
var.body.categories[0]['name'] = 'My new category'
var.edit(categories=var.body.categories)
```

## Ordering

#### Ordering variables

Ordering variables is as easy as rearranging the order of their 
respective URL's in the `ds.variables.hier.graph` list

```python
ds.variables.hier.graph = [var2.self, var1.self]
```

#### Grouping variables

You can group variables in *groups* by providing a dictionary in that list.
If we wanted to group `var1` and `var2` we can simply:

```python
group = {'My Awesome Group': [var1.self, var2.self]}
ds.variables.hier.graph = [group]
```

## Downloading datasets

Downloading datasets is as easy as:

```python
ds.download(
    path='/Dowloads/ds.csv', 
    filter='var1 == 4',
    variables=['var1, var2'],
    include_hidden=True
)
```

This download will be a CSV file with categories as id's and not labels, as the 
UI provides. The `filter` argument allows the filtering of rows based on 
expressions and defaults to all rows if `filter` is not provided as an
argument. `variables` allows to define which variables to include 
in the download only. This argument is a list of `alias`, `id` or
`variable objects`. The method defaults `include_hidden=True` but setting it 
to False will not include hidden variables in the downloaded file. When `variables`
is specified the `include_hidden` argument is ignored for obvious reasons.