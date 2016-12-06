Navigating the Crunch.io API with pycrunch
==========================================

This document intends to describe how to move around the different
elements and structures that the Crunch.io API exposes.

We are going to use the `site` session object previously described in the
Introduction section. It will be our intermediary between Python and
Crunch.io.


Catalogs
--------

Every collection of elements in Crunch is a Shoji Catalog. We
can quickly interact with the datasets Catalog by doing:

```python
site.datasets
```

Catalogs provide some methods to handle individual elements. Let's
say we want to interact with the dataset named "US Elections 2016".
The method `get_dataset()` uses the session (site) previously initiated and
retrieves datasets by it's name or id:

```python
from pycrunch import get_dataset
ds = get_dataset(dataset="US Elections 2016")
```

Now the variable `ds` holds a reference to the dataset 
we need to interact with. Just like we interacted with the datasets
Catalog, we can interact with the dataset variables Catalog as well:

```python
ds.variables
```

In order to visualize, for example, the data contents of our dataset 
we can make use of the table entity:

```python
ds.table.data
```

Pycrunch also allows interacting with data using Pandas. To access the
Pandas Dataframe of our dataset object, we would do the following:

```python
from pycrunch import pandaslib
df = pandaslib.dataframe(ds)
```
