Introduction
============

Every Crunch Dataset has a **Hierarchical Order** structure which determines
how variables are ordered and grouped in the left-hand side navigation pane
in the Crunch UI.

For example, a Dataset could have a **Hierarchical Order** structure
that renders in the Crunch UI like so:

```
id
hobbies
 Account
  registration_time
  last_login_time
     User Information
    first_name
    last_name
    gender
     Location
    country
    city
    zip_code
    address
music
religion
```

in which `id`, `hobbies`, `first_name`, `last_name`, `registration_time`,
`gender`, `country`, `city`, `zip_code`, `address`, `music` and `religion`
would be **variable names** (not aliases) whereas `Account`,
`User Information` and `Location` would be **group names**.

The [Crunch API](http://docs.crunch.io/#hierarchical-order) allows complete
manipulation of the **Hierarchical Order** structure of a Dataset using
standard HTTP verbs. However, the *JSON* payload objects needed for this
purpose can become quite big and difficult to work with.

The **scrunch** library provides a high-level API for manipulating such
structures in a convenient manner.

Quick Showcase of the API
-------------------------

This section provides a quick example of how working with the
**Hierarchical Order** looks like in **scrunch**.

Let's suppose that we wanted to modify the previous example of a
**Hierarchical Order** structure so that it looks like this:

```
id
zip_code
address
 Location
  country
  city
hobbies
music
 Account
  registration_time
  last_login_time
  gender
 User Information
  first_name
  last_name
religion
```

Assuming that the Dataset in question is loaded in a variable named
`ds`, and that all the variables have the `alias` field equal to the
`name` field, one would achieve the desired structure using **scrunch**
by doing:

```python
order = ds.order
order.insert_after('id', ['zip_code', 'address', 'Location'])
order.move_up('music')
order.insert_before('religion', 'User Information')
order['Account'].insert('gender')
```

Detailed information on the usage of the **Hierarchical Order** API in
**scrunch** is given in the rest of this document.

Hierarchical Order API
======================

In order to show how **scrunch**'s **Hierarchical Order** API works,
we'll use an example Dataset with the same **Hierarchical Order**
structure as the one in the Introduction section. That is:

```
id
hobbies
 Account
  registration_time
  last_login_time
     User Information
    first_name
    last_name
    gender
     Location
    country
    city
    zip_code
    address
music
religion
```

Additionally, every variable in this example Dataset is considered to have
the same value for both the `alias` and the `name` properties. Therefore,
the list of aliases in the example Dataset would be: `id`, `hobbies`,
`first_name`, `last_name`, `registration_time`, `gender`, `country`,
`city`, `zip_code`, `address`, `music` and `religion`.

We'll refer to the example Dataset using a variable named `ds`, obtained
in the following manner:

```python
from scrunch import connect, get_dataset
site = connect(user='me@mycompany.com', pw='your_password')
ds = get_dataset('Example Dataset', site=site)
```

Accessing the Hierarchical Order of a Dataset
---------------------------------------------

Every `Dataset` object in **scrunch** has an `order` property which
encapsulates the **Hierarchical Order** structure and provides the
high-level API to manipulate it.

Here's an example of how the `order` property is accessed:

```
>>> order = ds.order
>>> print(order)
[
    "id",
    "hobbies",
    "Group(Account)",
    "music",
    "religion"
]
```

Elements of the Hierarchical Order
----------------------------------

There are two types of elements contained within the `order` property:
- `scrunch.datasets.Variable` objects.
- `scrunch.datasets.Group` objects, which group together variables
   and other groups in a particular order. `Group` objects have a `name`
   property and a deterministic order in which its elements are arranged.

The `order` object itself encapsulates the **root** `Group`, which contains
all variables and sub-groups in the hierarchy.

Element Representation
----------------------

The string representation of `Group` objects, including the **root**
`Group`, does not drill down the hierarchy and thus shows only
first-level child elements, like so:

```
>>> print(ds.order)
[
    "id",
    "hobbies",
    "Group(Account)",
    "music",
    "religion"
]
```

`scrunch.datasets.Variable` elements are represented as strings using
their `alias` property.

`scrunch.datasets.Group` elements are also represented as strings, using
a `Group(name)` naming schema to differentiate them from `Variable`
elements.

Every `Group` object has two properties which provide some useful string
representations: `hierarchy` and `variables`.

The `hierarchy` property provides a (potentially) nested representation
of all the elements within the `Group`. For instance:

```
>>> print(ds.order.hierarchy)
[
    "id",
    "hobbies",
    {
        "Account": [
            "registration_time",
            "last_login_time",
            {
                "User Information": [
                    "first_name",
                    "last_name",
                    "gender"
                ]
            },
            {
                "Location": [
                    "country",
                    "city",
                    "zip_code",
                    "address"
                ]
            }
        ]
    },
    "music",
    "religion"
]
```

Note how sub-groups are displayed as python `dict` objects with a single
key, which is the sub-group's name.

The `variables` property provides a flat view of all variables within
the `Group`, like so:

```
>>> print(ds.order.variables)
[
    "id",
    "hobbies",
    "registration_time",
    "last_login_time",
    "first_name",
    "last_name",
    "gender",
    "country",
    "city",
    "zip_code",
    "address",
    "music",
    "religion"
]
```

As can be expected, the `hierarchy` and `variables` properties are
accessible for sub-groups:

```
>>> print(ds.order['Account'].hierarchy)
[
    "registration_time",
    "last_login_time",
    {
        "User Information": [
            "first_name",
            "last_name",
            "gender"
        ]
    },
    {
        "Location": [
            "country",
            "city",
            "zip_code",
            "address"
        ]
    }
]

>>> print(ds.order['Account'].variables)
[
    "registration_time",
    "last_login_time",
    "first_name",
    "last_name",
    "gender"
    "country",
    "city",
    "zip_code",
    "address"
]
```

More information on how group elements are accessed can be found in the
**Element Access** section of this document.

Element Access
--------------

`Group` objects allow access to their elements using python's square
bracket syntax in two forms:

- With 0-based integer indexes, like python `list` objects. For example:

```python
user_id = ds.oder[0]
```

- With string keys, as with python `dict` objects. In this case, the
  key must refer to either the `alias` of a variable or to the `name`
  of a sub-group. For example:

```python
user_id = ds.order['id']
```

Either an `IndexError` exception or a `KeyError` exception is raised when
an element is not found within the `Group` object.

Nested access is also possible:

```python
registration_time = ds.order['Account']['registration_time']
registration_time = ds.order[2][0]  # using indexes too!
```

The fetched elements are either `Variable` objects or `Group` objects:

```
>>> user_id = ds.order['id']
>>> type(user_id)
<class 'scrunch.datasets.Variable'>
>>> account_group = ds.order['Account']
>>> type(account_group)
<class 'scrunch.datasets.Group'>
```

The `hierarchy` and `variables` properties also allow the same kind of
element access as `Group` objects:

```python
registration_time = ds.order['Account'].hierarchy[0]
country = ds.order.variables['country']
```

Manipulating the Hierarchical Order of a Dataset
------------------------------------------------

### Group-level Reordering

`Group` objects have a `set` method that allows the reordering
of all first-level elements with a single call. Naturally, this also
applies to the **root** group.

For instance, the `set` method could be used to reorder the elements
of the **root** group in the following manner:

```python
ds.order.set(['id', 'music', 'religion', 'hobbies', 'Account'])
```

which should update the order structure like so:

```
id
music
religion
hobbies
 Account
  registration_time
  last_login_time
     User Information
    first_name
    last_name
    gender
     Location
    country
    city
    zip_code
    address
```

The only argument to the `set` method is a `list` which must contain the
new arrangement of elements at the same hierarchy level.

The following validations to the new arrangement of elements are enforced:
- The input list must only contain string elements that refer to
  aliases or sub-group names that are first-level elements of the
  `Group` being modified.
- All of the current first-level elements must be included in the input list.

A `ValueError` exception is raised if any of the above validations fail.

The `set` method works within nested groups. For example, the following code:

```python
ds.order['Account'].set(
    ['User Information', 'Location', 'last_login_time', 'registration_time']
)
```

would reorder the **Account** sub-group in our example dataset.

### Modifying the position of Elements in a Group

`Group` objects have a `move` method that allows moving an element to
a target position **within the group**.

The first argument is the `element` to move, given as a string reference
to either the `alias` of a variable or the `name` of a sub-group.

The second argument is the target `position`, which is expected to be
a 0-based integer index. If its value is `-1`, then the element is set to
be moved to the end of the order.

Examples of usage:

```python
ds.order.move('id', 3)
ds.order.move(element='Account', position=-1)
```

The `move_up`, `move_down`, `move_top` and `move_bottom` methods are also
provided as a convenience for dealing with very common operations. As can be
inferred, these methods only need a reference to the element to be moved
about:

```python
ds.order.move_bottom('id')
ds.order.move_top(element='Account')
ds.order['Account'].move_down('registration_time')
```

Additionally, two useful methods are provided: `move_before` and
`move_after`. Both of these take a `reference` element as their first
argument, and the `element` to be moved as the second argument. The
`reference` parameter must be the `alias` of a variable or the `name`
of a sub-group **within the group**. For instance:

```python
ds.order.move_before('Account', 'id')
ds.order.move_after(reference='Account', element='religion')
ds.order['Account'].move_after('Location', 'registration_time')
```

### Inserting Elements from other Groups

Sometimes it is needed to _migrate_ one or more elements to a specific
`Group` within the hierarchy. To that end, `Group` objects provide the
`insert` method.

In its simplest form, `insert` allows inserting an element at the bottom
of the group's order, effectively removing it from it previous
group (**root** included).

Using our example dataset (in its initial state), we could do:

```python
ds.order['Account'].insert('id')
```

which would update the hierarchical order structure like so:

```
hobbies
 Account
  registration_time
  last_login_time
     User Information
    first_name
    last_name
    gender
     Location
    country
    city
    zip_code
    address
  id
music
religion
```

The first argument of the `insert` method is the list of `elements`
to _migrate_. Each element can be either the `alias` of a variable or the
`name` of a group. If a single element (string) is provided as the first
argument, then it is automatically converted to a one-element list.

The second (optional) argument is the target `position` at which the
element(s) will inserted, which is expected to be a 0-based integer index.
Its default value is `-1`, which means that the element(s) will be inserted at
the end (bottom) of the group's order.

More examples:

```python
ds.order.insert(['address', 'gender'], 1)
ds.order.insert(elements=['address', 'gender'], position=0)
ds.order.insert(elements='first_name', position=3)
```

Additionally, two useful methods are provided: `insert_before` and
`insert_after`. Both of these take a `reference` element as their first
argument, and the list of `elements` to be _migrated_ as the second
argument. The `reference` parameter must be the `alias` of a variable
or the `name` of a sub-group **within the group**. For instance:

```python
ds.order.insert_before('Account', 'id')
ds.order.insert_after(reference='Account', elements='religion')
ds.order.insert_after(reference='Account', elements=['religion', 'gender'])
ds.order['Account'].insert_after('Location', elements=['id', 'religion'])
```

### Removing Elements from Groups

`Group` objects provide a `remove` method that allows getting rid of
elements within it. Target elements are removed from the group and
moved to the **root** group (at the end).

The first (and only) argument is the list of `elements` to remove
from the new group. Each of these elements can be either the `alias` of
a variable or the `name` of a sub-group (**within the group**). If a
single element (string) is provided then it is automatically converted
to a one-element list. Examples:

```python
ds.order['Account'].remove('registration_time')
ds.order['Account'].remove(['registration_time', 'Location'])
ds.order['Account'].remove(elements=['registration_time', 'Location'])
```

Trying to remove elements from the **root** group raises a
`NotImplementedError` exception.

### Creating new Groups

The **Hierarchical Order** structure always has at least one main group
which contains the top-level variables in the hierarchy; it can also
have nested sub-groups with more variables within them. The
**scrunch** library terms this main group the **_root_** group.

New groups can be inserted into the hierarchy by means of the `create`
method provided by existing `Group` objects (and we know there is
at least one of them: the **root** group).

The `create` method takes the `name` of the new group as its first
argument:

```python
ds.order.create('new group')
ds.order.order['Account'].create(name='another group')
```

A second (and optional) argument is the list of `elements` to _migrate_
to the new group. Each of these elements can be either the `alias` of
a variable or the `name` of a group. If a single element (string) is provided
then it is automatically converted to a one-element list. Examples:


```python
ds.order.create('new group', 'id')
ds.order.order['Account'].create(name='my group', elements=['id', 'gender'])
```

### Deleting existing Groups

The `delete` method in `Group` objects takes care of deleting the group
from the hierarchical order structure. Before doing so, it first
migrates any existing element to the **root** group.

This method has no arguments. Trying to `delete` the **root** group
raises a `NotImplementedError` exception.

Example:

```python
ds.order['Account'].delete()
```

Miscellaneous Utilities
-----------------------

`Group` objects provide two very useful methods: `find` and `find_group`.

The `find` method looks for an `alias` within the group (including nested
sub-groups) and returns its _container_ `Group` object. If the target
`alias` is nowhere to be found, then `None` is returned.

The `find` method is most useful when used on the **root** group
to look for any `alias` within the whole hierarchical order structure:

```python
if ds.order.find('gender'):
    print('FOUND')

container = ds.order.find('id')
if container:
    user_id = container['id']
```

The `find_group` method works similarly, but it looks for sub-groups by
`name`. The `Group` object is returned in the case of success. If the
group is not found, then `None` is returned:

```python
if ds.order.find_group('Account'):
    print('FOUND')
```


