Introduction
============

The following document describes the basic usage of the pycrunch library.

Getting started
---------------

Start a simple Crunch session:

```python
import pycrunch
site = pycrunch.connect(
    user="me@mycompany.com", 
    pw="yourpassword", 
    site_url="https://app.crunch.io/api/",
    project="US Election"
)
```

The argument `project` indicates which Crunch project should the scope
of the session be based on.

A session can also be created with a token:

```python
import pycrunch
site = pycrunch.connect_with_token(
    token="token", 
    site_url="https://app.crunch.io/api/",
    project="US Elections"
)
```
