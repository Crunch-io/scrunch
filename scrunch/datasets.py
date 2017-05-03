import collections
import datetime
import json
import logging
import os
import re
import sys

import pycrunch
import six

from pycrunch.exporting import export_dataset
from pycrunch.importing import Importer
from pycrunch.shoji import wait_progress
from scrunch.categories import CategoryList
from scrunch.exceptions import (AuthenticationError, InvalidPathError,
                                InvalidReferenceError, OrderUpdateError)
from scrunch.expressions import parse_expr, prettify, process_expr
from scrunch.helpers import (ReadOnly, _validate_category_rules, abs_url,
                             case_expr, download_file, subvar_alias)
from scrunch.variables import (combinations_from_map, combine_categories_expr,
                               combine_responses_expr, responses_from_map)

import pandas as pd

if six.PY2:  # pragma: no cover
    import ConfigParser as configparser
    from urlparse import urlsplit
else:
    import configparser
    from urllib.parse import urlsplit

_VARIABLE_PAYLOAD_TMPL = {
    'element': 'shoji:entity',
    'body': {
        'name': 'name',
        'description': 'description',
        'alias': 'alias'
    }
}

_MR_TYPE = 'multiple_response'

LOG = logging.getLogger('scrunch')


def _set_debug_log():
    # ref: http://docs.python-requests.org/en/master/api/#api-changes
    #
    #  These two lines enable debugging at httplib level
    # (requests->urllib3->http.client)
    # You will see the REQUEST, including HEADERS and DATA,
    # and RESPONSE with HEADERS but without DATA.
    # The only thing missing will be the response.body which is not logged.
    try:
        import http.client as http_client
    except ImportError:
        # Python 2
        import httplib as http_client
    http_client.HTTPConnection.debuglevel = 1
    LOG.setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def _get_connection(file_path='crunch.ini'):
    """
    Utilitarian function that reads credentials from
    file or from ENV variables
    """
    if pycrunch.session is not None:
        return pycrunch.session
    # try to get credentials from environment
    username = os.environ.get('CRUNCH_USERNAME')
    password = os.environ.get('CRUNCH_PASSWORD')
    site = os.environ.get('CRUNCH_URL')
    if username and password and site:
        LOG.debug("Found Crunch credentials on Environment")
        return pycrunch.connect(username, password, site)
    elif username and password:
        LOG.debug("Found Crunch credentials on Environment")
        return pycrunch.connect(username, password)
    # try reading from .ini file
    config = configparser.ConfigParser()
    config.read(file_path)
    try:
        username = config.get('DEFAULT', 'CRUNCH_USERNAME')
        password = config.get('DEFAULT', 'CRUNCH_PASSWORD')
    except:
        username = password = None
    try:
        site = config.get('DEFAULT', 'CRUNCH_URL')
    except:
        site = None
    # now try to login with obtained creds
    if username and password and site:
        LOG.debug("Found Crunch credentials on crunch.ini")
        return pycrunch.connect(username, password, site)
    elif username and password:
        LOG.debug("Found Crunch credentials on crunch.ini")
        return pycrunch.connect(username, password)
    else:
        raise AuthenticationError(
            "Unable to find crunch session, crunch.ini file or environment variables.")


def get_dataset(dataset, connection=None, editor=False, project=None):
    """
    Retrieve a reference to a given dataset (either by name, or ID) if it exists
    and the user has access permissions to it. If you have access to the dataset
    through a project you should do pass the project parameter.

    This method tries to use pycrunch singleton connection, environment variables
    or a crunch.ini config file if the optional "connection" parameter isn't provided.

    Also able to change editor while getting the dataset with the optional
    editor parameter.

    Returns a Dataset Entity record if the dataset exists.
    Raises a KeyError if no such dataset exists.
    """
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")
    root = connection
    if project:
        if isinstance(project, six.string_types):
            project_obj = get_project(project, connection)
            ds = project_obj.get_dataset(dataset)
        else:
            ds = project.get_dataset(dataset)
    else:
        try:
            shoji_ds = root.datasets.by('name')[dataset].entity
        except KeyError:
            try:
                shoji_ds = root.datasets.by('id')[dataset].entity
            except KeyError:
                raise KeyError("Dataset (name or id: %s) not found in context." % dataset)

        ds = Dataset(shoji_ds)

    if editor is True:
        ds.change_editor(root.session.email)

    return ds


def get_project(project, connection=None):
    """
    :param project: Crunch project ID or Name
    :param connection: An scrunch session object
    :return: Project class instance
    """
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")
    try:
        ret = connection.projects.by('name')[project].entity
    except KeyError:
        try:
            ret = connection.projects.by('id')[project].entity
        except KeyError:
            raise KeyError("Project (name or id: %s) not found." % project)
    return Project(ret)


def get_user(user, connection=None):
    """
    :param user: Crunch user email address
    :param connection: An scrunch session object
    :return: User class instance
    """
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")
    try:
        ret = connection.users.by('email')[user].entity
    except KeyError:
        raise KeyError("User email '%s' not found." % user)
    return User(ret)


def create_dataset(name, variables, connection=None):
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")

    shoji_ds = connection.datasets.create({
        'element': 'shoji:entity',
        'body': {
            'name': name,
            'table': {
                'element': 'crunch:table',
                'metadata': variables
            }
        }
    }).refresh()
    return Dataset(shoji_ds)


class User:
    _MUTABLE_ATTRIBUTES = {'name', 'email'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, user_resource):
        self.resource = user_resource
        self.url = self.resource.self

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

        # Attribute doesn't exists, must raise an AttributeError
        raise AttributeError('User has no attribute %s' % item)

    def __repr__(self):
        return "<User: email='{}'; id='{}'>".format(self.email, self.id)

    def __str__(self):
        return self.email


class Project:
    _MUTABLE_ATTRIBUTES = {'name', 'description', 'icon'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, project_resource):
        self.resource = project_resource
        self.url = self.resource.self

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

        # Attribute doesn't exists, must raise an AttributeError
        raise AttributeError('Project has no attribute %s' % item)

    def __repr__(self):
        return "<Project: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name

    def get_dataset(self, dataset):
        try:
            shoji_ds = self.resource.datasets.by('name')[dataset].entity
        except KeyError:
            try:
                shoji_ds = self.resource.datasets.by('id')[dataset].entity
            except KeyError:
                raise KeyError(
                    "Dataset (name or id: %s) not found in project." % dataset)

        ds = Dataset(shoji_ds)
        return ds

    @property
    def users(self):
        """
        :return: dictionary of User instances
        """
        # TODO: return a dictionary keyed by email and values should be User
        # instances, but when trying to got 403 from Crunch
        return [e['email'] for e in self.resource.members.index.values()]
        # return {val['email']: User(val.entity) for val in self.resource.members.index.values()}

    def remove_user(self, user):
        """
        :param user: email or User instance
        :return: None
        """
        if not isinstance(user, User):
            user = get_user(user)

        found_url = None
        for url, tuple in self.resource.members.index.items():
            if tuple['email'] == user.email:
                found_url = url

        if found_url:
            self.resource.members.patch({found_url: None})
        else:
            raise KeyError("User %s not found in project %s" % (user.email, self.name))

    def add_user(self, user, edit=False):
        """
        :param user: email or User instance
        :return: None
        """
        if not isinstance(user, User):
            user = get_user(user)
        self.resource.members.patch({user.url: {'edit': edit}})

    def edit_user(self, user, edit):
        if not isinstance(user, User):
            user = get_user(user)
        self.resource.members.patch(
            {user.url: {'permissions': {'edit': edit}}}
        )


class Path(object):
    def __init__(self, path):
        if not isinstance(path, six.string_types):
            raise TypeError('The path must be a string object')

        if not re.match(r'^\|$|^\|?([\w\s]+\|?)+$', path, re.UNICODE):
            raise InvalidPathError(
                'Invalid path %s: it contains invalid characters.' % path
            )

        self.path = path

    @property
    def is_root(self):
        return self.path == '|'

    @property
    def is_absolute(self):
        return self.path.startswith('|')

    @property
    def is_relative(self):
        return not self.is_absolute

    def get_parts(self):
        return [part.strip() for part in self.path.split('|') if part]

    def __str__(self):
        return self.path

    def __repr__(self):
        return self.__str__()


class Group(object):

    INDENT_SIZE = 4

    def __init__(self, obj, order, parent=None):
        self.name = list(obj.keys())[0]
        self.order = order
        self.parent = parent
        self.elements = collections.OrderedDict()

        # Load all the elements.
        for element in obj[self.name]:
            if isinstance(element, six.string_types):
                _id = element.split('/')[-2]
                var = self.order.vars.get(_id)
                if var:
                    self.elements[var['alias']] = var
            elif isinstance(element, Variable):
                self.elements[element.alias] = element
            else:
                subgroup = Group(element, order=self.order, parent=self)
                self.elements[subgroup.name] = subgroup

    def __str__(self):
        def _get_elements(group):
            elements = []
            for key, obj in list(group.elements.items()):
                if isinstance(obj, Group):
                    elements.append({key: _get_elements(obj)})
                elif isinstance(obj, Variable):
                    elements.append(obj.name)
                else:
                    elements.append(obj['name'])
            return elements

        str_elements = _get_elements(self)
        return json.dumps(str_elements, indent=self.INDENT_SIZE)

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        return self.itervalues()

    def itervalues(self):
        for item in self.elements.values():
            yield item

    def iterkeys(self):
        for key in self.elements.keys():
            yield key

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return zip(self.iterkeys(), self.itervalues())

    def __getitem__(self, path):
        if not isinstance(path, six.string_types):
            raise TypeError('arg 1 must be a string')

        path = Path(path)

        if path.is_root and self.is_root:
            return self

        if path.is_absolute and not self.is_root:
            raise InvalidPathError(
                'Absolute paths can only be used on the root Group.'
            )

        group = self
        for part in path.get_parts():
            try:
                group = group.elements[part]
            except KeyError:
                raise InvalidPathError(
                    'Invalid path %s: element %s does not exist.' % (path, part)
                )
            except AttributeError:
                raise InvalidPathError(
                    'Invalid path %s: element %s is not a Group.' % (path, part)
                )
            if not isinstance(group, Group):
                raise InvalidPathError(
                    'Invalid path %s: element %s is not a Group.' % (path, part)
                )

        return group

    def __contains__(self, item):
        return item in self.elements and isinstance(self.elements[item], Group)

    @property
    def is_root(self):
        return self.parent is None and self.name == '__root__'

    @staticmethod
    def _validate_alias_arg(alias):
        if isinstance(alias, six.string_types):
            alias = [alias]
        if not isinstance(alias, collections.Iterable):
            raise ValueError(
                'Invalid list of aliases/groups to be inserted into the Group.'
            )
        if not all(isinstance(a, six.string_types) for a in alias):
            raise ValueError(
                'Only string references to aliases/group names are allowed.'
            )
        return alias

    def _validate_name_arg(self, name):
        if not isinstance(name, six.string_types):
            raise ValueError(
                'The name argument must be a string object.'
            )
        if len(name) > 40:
            raise ValueError(
                'The name argument must not be longer than 40 characters.'
            )
        if '|' in name:
            raise ValueError(
                'The pipe (|) character is not allowed.'
            )
        if not re.match(r'^[\w\s]+$', name, re.UNICODE):
            raise ValueError(
                'Invalid name %s: it contains characters that are not allowed.'
                % name
            )
        if name in self.elements:
            raise ValueError(
                'A variable/sub-group named \'%s\' already exists.' % name
            )
        return name

    def _validate_reference_arg(self, reference):
        if not isinstance(reference, six.string_types):
            raise TypeError('Invalid reference. It must be a string.')
        if reference not in self.elements:
            raise InvalidReferenceError(
                'Invalid reference %s: it is not part of the current Group.'
                % reference
            )
        return reference

    def find(self, name):

        def _find(group):
            for _name, obj in group.elements.items():
                if isinstance(obj, Group):
                    result = _find(obj)
                    if result:
                        return result
                elif _name == name:
                    return group

        return _find(self)

    def find_group(self, name):

        def _find(group):
            if group.name == name:
                return group
            for _name, obj in group.elements.items():
                if isinstance(obj, Group):
                    result = _find(obj)
                    if result:
                        return result

        return _find(self)

    def insert(self, alias, position=0, before=None, after=None):
        elements = self._validate_alias_arg(alias)

        if not isinstance(position, int):
            raise ValueError('Invalid position. It must be an integer.')
        if position < -1 or position > len(self.elements):
            raise IndexError('Invalid position %d' % position)
        if position == 0 and (before or after):
            reference = self._validate_reference_arg(before or after)
            i = 0
            for name in self.elements.keys():
                if name in elements:
                    continue
                if name == reference:
                    if before:
                        position = i
                    elif after:
                        position = i + 1
                    break
                i += 1
        if position == -1:
            position = len(self.elements)

        elements_to_move = collections.OrderedDict()
        for element_name in elements:
            if element_name in self.elements:
                elements_to_move[element_name] = \
                    (self.elements[element_name], '__move__')
            else:
                current_group = self.order.graph.find(element_name)
                if current_group:
                    # A variable.
                    elements_to_move[element_name] = \
                        (current_group, '__migrate_var__')
                else:
                    # Not a variable. A group, maybe?
                    group_to_move = self.order.graph.find_group(element_name)
                    if group_to_move:
                        elements_to_move[element_name] = \
                            (group_to_move, '__migrate_group__')
                    else:
                        raise ValueError(
                            'Invalid alias/group name \'%s\'' % element_name
                        )

        # Make all necessary changes to the order structure.
        _elements = collections.OrderedDict()
        _non_targeted = list()

        for name in list(self.elements.keys()):
            if name in elements_to_move:
                continue
            _non_targeted.append((name, self.elements[name]))
        _non_targeted = list(reversed(_non_targeted))  # FIFO for .pop()

        total = len(_non_targeted) + len(elements_to_move)
        i = 0
        while i <= total:
            if i == position:
                for element_name in elements_to_move.keys():
                    obj, operation = elements_to_move[element_name]

                    if operation == '__move__':
                        _elements[element_name] = obj
                    elif operation == '__migrate_var__':
                        current_group = obj
                        element = current_group.elements[element_name]
                        del current_group.elements[element_name]
                        _elements[element_name] = element
                    elif operation == '__migrate_group__':
                        group_to_move = obj
                        orig_parent = group_to_move.parent
                        group_to_move.parent = self
                        del orig_parent.elements[element_name]
                        _elements[element_name] = group_to_move
            elif len(_non_targeted):
                element_name, obj = _non_targeted.pop()
                _elements[element_name] = obj

            i += 1

        self.elements = _elements

        # Update!
        self.order.update()

    def append(self, alias):
        self.insert(alias, position=-1)

    def reorder(self, items):
        existing_items = [name for name in self.elements.keys()]
        if len(items) != len(existing_items) or \
                not all(i in existing_items for i in items):
            raise ValueError('Invalid list of items for the reorder operation.')

        if items == existing_items:
            # Nothing to do.
            return

        _elements = collections.OrderedDict()
        for item in items:
            _elements[item] = self.elements[item]
        self.elements = _elements

        self.order.update()

    def create_group(self, name, alias=None):
        name = self._validate_name_arg(name)
        elements = self._validate_alias_arg(alias)

        # Locate all elements to move. All of them have to exist.
        elements_to_move = collections.OrderedDict()
        for element_name in elements:
            current_group = self.order.graph.find(element_name)
            if current_group and element_name in current_group.elements:
                elements_to_move[element_name] = (current_group,)
            else:
                group_to_move = self.order.graph.find_group(element_name)
                if group_to_move:
                    elements_to_move[element_name] = group_to_move
                else:
                    raise ValueError(
                        'Invalid alias/group name \'%s\'' % element_name
                    )

        # Make the modifications to the order structure.
        new_group = Group({name: []}, order=self.order, parent=self)
        for element_name, obj in elements_to_move.items():
            if isinstance(obj, tuple):
                current_group = obj[0]
                new_group.elements[element_name] = \
                    current_group.elements[element_name]
                del current_group.elements[element_name]
            else:
                group_to_move = obj
                orig_parent = group_to_move.parent
                group_to_move.parent = new_group
                new_group.elements[element_name] = group_to_move
                del orig_parent.elements[element_name]
        self.elements[name] = new_group

        # Update!
        self.order.update()

    def rename(self, name):
        name = self._validate_name_arg(name)

        if self.is_root:
            raise ValueError(
                'Renaming the root Group is not allowed.'
            )

        if name in self.parent.elements:
            raise ValueError(
                'Parent Group \'%s\' already contains an element named \'%s\'.'
                % (self.parent.name, name)
            )

        if name == self.name:
            # Nothing to do.
            return

        # Rename!
        _elements = collections.OrderedDict()
        for current_name, obj in self.parent.elements.items():
            if current_name == self.name:
                _elements[name] = obj
            else:
                _elements[current_name] = obj
        self.name = name
        self.parent.elements = _elements

        # Update!
        self.order.update()

    def move(self, path, position=-1):
        path = Path(path)
        if not path.is_absolute:
            raise InvalidPathError(
                'Invalid path %s: only absolute paths are allowed.' % path
            )

        target_group = self.order[str(path)]
        if target_group == self:
            raise InvalidPathError(
                'Invalid path %s: cannot move Group into itself.' % path
            )
        target_group.insert(self.name, position=position)


class Order(object):

    def __init__(self, ds):
        self.ds = ds
        self._hier = None
        self._vars = None
        self._graph = None
        self._sync = True
        self._revision = None

    def _load_hier(self):
        self._hier = self.ds.resource.session.get(
            self.ds.resource.variables.orders.hier
        ).payload
        return self._hier

    def _load_vars(self):
        self._vars = self.ds.resource.variables.by('id')
        return self._vars

    def _load_graph(self):
        self._graph = Group({'__root__': self.hier.graph}, order=self)
        return self._graph

    @property
    def hier(self):
        if self._hier is None:
            self._load_hier()
        return self._hier

    @hier.setter
    def hier(self, _):
        raise TypeError('Unsupported assignment operation')

    @property
    def vars(self):
        if self._vars is None:
            self._load_vars()
        return self._vars

    @vars.setter
    def vars(self, _):
        raise TypeError('Unsupported assignment operation')

    @property
    def graph(self):
        if self._graph is None:
            self._load_graph()
        return self._graph

    @graph.setter
    def graph(self, _):
        raise TypeError('Unsupported assignment operation')

    def get(self):
        # Returns the synchronized hierarchical order graph.
        if self._sync:
            ds_state = self.ds.resource.session.get(
                self.ds.resource.self + 'state/'
            ).payload
            if self._revision is None:
                self._revision = ds_state.body.revision
            elif self._revision != ds_state.body.revision:
                # There's a new dataset revision. Reload the
                # hierarchical order.
                self._revision = ds_state.body.revision
                self._load_hier()
                self._load_graph()
        return self

    def _build_graph_structure(self):

        def _get(group):
            _elements = []
            for obj in group.elements.values():
                if isinstance(obj, Group):
                    _elements.append({
                        obj.name: _get(obj)
                    })
                else:
                    if isinstance(obj, Variable):
                        _id = obj.id
                    else:
                        _id = obj['id']
                    _elements.append('../%s/' % _id)
            return _elements

        return _get(self.graph)

    def update(self):
        updated_order = {
            'element': 'shoji:order',
            'graph': self._build_graph_structure()
        }
        try:
            self.hier.put(updated_order)
        except (pycrunch.ClientError, pycrunch.ServerError) as e:
            # Our update to the Hierarchical Order failed. Better reload.
            self._hier = self._vars = self._graph = None
            self._load_graph()
            raise OrderUpdateError(str(e))

    # Proxy methods for the __root__ Group

    def __str__(self):
        return str(self.graph)

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        return self.graph.itervalues()

    def itervalues(self):
        return self.graph.itervalues()

    def iterkeys(self):
        return self.graph.iterkeys()

    def keys(self):
        return self.graph.keys()

    def values(self):
        return self.graph.values()

    def items(self):
        return self.graph.items()

    def __getitem__(self, item):
        return self.graph[item]


class Filter:
    """
    A pycrunch.shoji.Entity for Dataset filters
    """
    _MUTABLE_ATTRIBUTES = {'name', 'expression', 'is_public', 'owner_id'}
    _IMMUTABLE_ATTRIBUTES = {'id', }
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, shoji_tuple):
        self.resource = shoji_tuple.entity

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]
        raise AttributeError('Filter has no attribute %s' % item)

    def __repr__(self):
        return "<Filter: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name

    def edit(self, **kwargs):
        for key in kwargs:
            if key not in self._MUTABLE_ATTRIBUTES:
                raise AttributeError("Can't edit attribute %s of 'filter' %s" % (
                    key, self.name
                ))
        return self.resource.edit(**kwargs)

    def remove(self):
        self.resource.delete()


class CrunchBox(object):
    """
    A CrunchBox representation of boxdata.

    an instance cannot mutate it's metadata directly since boxdata doesn't
    support PATCHing. Instead, simply create a new `CrunchBox` instance with
    the same Filters and Variables. You'll get the same entity from the boxdata
    index with the updated metadata.

    :param shoji_tuple: pycrunch.shoji.Tuple of boxdata
    :param     dataset: scrunch.datasets.Dataset instance

    NOTE: since the boxdata entity is different regarding the mapping of body
          and metadata fields, methods etc... it is made `readonly`.
          Since an `edit` method would need to return a new
          instance (see above) the `__setattr__` method ist incorporated with
          CrunchBox specific messages.

          (an edit method returning an instance would most likely brake user
          expectations)

          In order to have a proper `remove` method we also need the Dataset
          instance.
    """

    WIDGET_URL = 'https://s.crunch.io/widget/index.html#/ds/{id}/'
    DIMENSIONS = dict(height=480, width=600)

    # the attributes on entity.body.metadata
    _METADATA_ATTRIBUTES = {'title', 'notes', 'header', 'footer'}

    _MUTABLE_ATTRIBUTES = _METADATA_ATTRIBUTES

    _IMMUTABLE_ATTRIBUTES = {
        'id', 'user_id', 'creation_time', 'filters', 'variables'}

    # removed `dataset` from the set above since it overlaps with the Dataset
    # instance on self. `boxdata.dataset` simply points to the dataset url

    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, shoji_tuple, dataset):
        self.resource = shoji_tuple
        self.url = shoji_tuple.entity_url
        self.dataset = dataset

    def __setattr__(self, attr, value):
        """ known attributes should be readonly """

        if attr in self._IMMUTABLE_ATTRIBUTES:
            raise AttributeError(
                "Can't edit attibute '%s'" % attr)
        if attr in self._MUTABLE_ATTRIBUTES:
            raise AttributeError(
                "Can't edit '%s' of a CrunchBox. Create a new one with "
                "the same filters and variables to update its metadata" % attr)
        object.__setattr__(self, attr, value)

    def __getattr__(self, attr):
        if attr in self._METADATA_ATTRIBUTES:
            return self.resource.metadata[attr]

        if attr == 'filters':
            # return a list of `Filters` instead of the filters expr on `body`
            _filters = []
            for obj in self.resource.filters:
                f_url = obj['filter']
                _filters.append(
                    Filter(self.dataset.resource.filters.index[f_url]))
            return _filters

        if attr == 'variables':
            # return a list of `Variables` instead of the where expr on `body`
            _var_urls = []
            _var_map = self.resource.where.args[0].map
            for v in _var_map:
                _var_urls.append(_var_map[v]['variable'])

            return [
                Variable(entity, self.dataset)
                for url, entity in self.dataset._vars
                if url in _var_urls
            ]

        # all other attributes not catched so far
        if attr in self._ENTITY_ATTRIBUTES:
            return self.resource[attr]
        raise AttributeError('CrunchBox has no attribute %s' % attr)

    def __repr__(self):
        return "<CrunchBox: title='{}'; id='{}'>".format(
            self.title, self.id)

    def __str__(self):
        return self.title

    def remove(self):
        self.dataset.resource.session.delete(self.url)

    @property
    def widget_url(self):
        return self.WIDGET_URL.format(id=self.id)

    @widget_url.setter
    def widget_url(self, _):
        """ prevent edits to the widget_url """
        raise AttributeError("Can't edit 'widget_url' of a CrunchBox")

    def iframe(self, logo=None, dimensions=None):
        dimensions = dimensions or self.DIMENSIONS
        widget_url = self.widget_url

        if not isinstance(dimensions, dict):
            raise TypeError('`dimensions` needs to be a dict')

        def _figure(html):
            return '<figure style="text-align:left;" class="content-list-'\
                   'component image">' + '  {}'.format(html) + \
                   '</figure>'

        _iframe = (
            '<iframe src="{widget_url}" width="{dimensions[width]}" '
            'height="{dimensions[height]}" style="border: 1px solid #d3d3d3;">'
            '</iframe>')

        if logo:
            _img = '<img src="{logo}" stype="height:auto; width:200px;'\
                   ' margin-left:-4px"></img>'
            _iframe = _figure(_img) + _iframe

        elif self.title:
            _div = '<div style="padding-bottom: 12px">'\
                   '    <span style="font-size: 18px; color: #444444;'\
                   ' line-height: 1;">' + self.title + '</span>'\
                   '  </div>'
            _iframe = _figure(_div) + _iframe

        return _iframe.format(**locals())


class DatasetSettings(dict):

    def __readonly__(self, *args, **kwargs):
        raise RuntimeError('Please use the change_settings() method instead.')

    __setitem__ = __readonly__
    __delitem__ = __readonly__
    pop = __readonly__
    popitem = __readonly__
    clear = __readonly__
    update = __readonly__
    setdefault = __readonly__
    del __readonly__


class DatasetVariablesMixin(collections.Mapping):
    """
    Handles dataset variable iteration in a dict-like way
    """

    def __getitem__(self, item):
        # Check if the attribute corresponds to a variable alias
        variable = self.resource.variables.by('alias').get(item)
        if variable is None:
            # Variable doesn't exists, must raise a ValueError
            raise ValueError('Dataset %s has no variable with an alias %s' % (
                self.name, item))
        # Variable exists!, return the variable Instance
        return Variable(variable, self)

    def _reload_variables(self):
        """
        Helper that takes care of updating self._vars on init and
        whenever the dataset adds a variable
        """
        self._vars = self.resource.variables.index.items()

    def __iter__(self):
        for var in self._vars:
            yield var

    def __len__(self):
        return len(self._vars)

    def itervalues(self):
        for _, var_tuple in self._vars:
            yield Variable(var_tuple, self)

    def iterkeys(self):
        for var in self._vars:
            yield var[1].name

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return zip(self.iterkeys(), self.itervalues())


class Dataset(ReadOnly, DatasetVariablesMixin):
    """
    A pycrunch.shoji.Entity wrapper that provides dataset-specific methods.
    """
    _MUTABLE_ATTRIBUTES = {'name', 'notes', 'description', 'is_published',
                           'archived', 'end_date', 'start_date'}
    _IMMUTABLE_ATTRIBUTES = {'id', 'creation_time', 'modification_time'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES
    _EDITABLE_SETTINGS = {'viewers_can_export', 'viewers_can_change_weight',
                          'viewers_can_share'}

    def __init__(self, resource):
        """
        :param resource: Points to a pycrunch Shoji Entity for a dataset.
        """
        super(Dataset, self).__init__(resource)
        self._settings = None
        # The `order` property, which provides a high-level API for
        # manipulating the "Hierarchical Order" structure of a Dataset.
        self._order = Order(self)
        # since we no longer have an __init__ on DatasetVariablesMixin because
        # of the multiple inheritance, we just initiate self._vars here
        self._reload_variables()

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist
        # Default behaviour
        return object.__getattribute__(self, item)

    def __repr__(self):
        return "<Dataset: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name

    @property
    def editor(self):
        try:
            return User(self.resource.follow('editor_url'))
        except pycrunch.lemonpy.ClientError:
            return self.resource.body.current_editor

    @editor.setter
    def editor(self, _):
        # Protect the `editor` from external modifications.
        raise TypeError(
            'Unsupported operation on the editor property'
        )

    @property
    def owner(self):
        owner_url = self.resource.body.owner
        try:
            if '/users/' in owner_url:
                return User(self.resource.follow('owner_url'))
            else:
                return Project(self.resource.follow('owner_url'))
        except pycrunch.lemonpy.ClientError:
            return owner_url

    @owner.setter
    def owner(self, _):
        # Protect `owner` from external modifications.
        raise TypeError(
            'Unsupported operation on the owner property'
        )

    def change_owner(self, user=None, project=None):
        """
        :param user: email or User object
        :param project: id, name or Project object
        :return:
        """
        if user and project:
            raise AttributeError(
                "Must provide user or project. Not both"
            )
        owner_url = None
        if user:
            if not isinstance(user, User):
                user = get_user(user)
            owner_url = user.url
        if project:
            if not isinstance(project, Project):
                project = get_project(project)
            owner_url = project.url

        if not owner_url:
            raise AttributeError("Can't set owner")

        self.resource.patch({'owner': owner_url})

    @property
    def order(self):
        return self._order.get()

    @order.setter
    def order(self, _):
        # Protect the `order` from external modifications.
        raise TypeError(
            'Unsupported operation on the Hierarchical Order property'
        )

    @property
    def settings(self):
        if self._settings is None:
            self._load_settings()
        return self._settings

    @settings.setter
    def settings(self, _):
        # Protect the `settings` property from external modifications.
        raise TypeError('Unsupported operation on the settings property')

    @property
    def filters(self):
        _filters = {}
        for f in self.resource.filters.index.values():
            filter_inst = Filter(f)
            _filters[filter_inst.name] = filter_inst
        return _filters

    @filters.setter
    def filters(self, _):
        # Protect the `settings` property from external modifications.
        raise TypeError('Use add_filter method to add filters')

    @property
    def crunchboxes(self):
        _crunchboxes = []
        for shoji_tuple in self.resource.boxdata.index.values():
            _crunchboxes.append(CrunchBox(shoji_tuple, self))
        return _crunchboxes

    @crunchboxes.setter
    def crunchboxes(self, _):
        # Protect the `crunchboxes` property from direct modifications
        raise TypeError('Use the `create_crunchbox` method to add one')

    def _load_settings(self):
        settings = self.resource.session.get(
            self.resource.fragments.settings).payload
        self._settings = DatasetSettings(
            (_name, _value) for _name, _value in settings.body.items()
        )
        return self._settings

    def change_settings(self, **kwargs):
        incoming_settings = set(kwargs.keys())
        invalid_settings = incoming_settings.difference(self._EDITABLE_SETTINGS)
        if invalid_settings:
            raise ValueError(
                'Invalid or read-only settings: %s'
                % ','.join(list(invalid_settings))
            )

        settings_payload = {
            setting: kwargs[setting] for setting in incoming_settings
        }
        if settings_payload:
            self.resource.session.patch(
                self.resource.fragments.settings,
                json.dumps(settings_payload),
                headers={'Content-Type': 'application/json'}
            )
            self._settings = None

    def edit(self, **kwargs):
        for key in kwargs:
            if key not in self._MUTABLE_ATTRIBUTES:
                raise AttributeError("Can't edit attibute %s of variable %s" % (
                    key, self.name
                ))
            if key in ['start_date', 'end_date'] and \
                    (isinstance(kwargs[key], datetime.date) or
                     isinstance(kwargs[key], datetime.datetime)
                     ):
                kwargs[key] = kwargs[key].isoformat()

        return self.resource.edit(**kwargs)

    def delete(self):
        """
        Delete a dataset.
        TODO: Shouldn't be possible if this is a streamed dataset.
        :return:
        """
        logging.debug("Deleting dataset %s (%s)." % (self.name, self.id))
        self.resource.delete()
        logging.debug("Deleted dataset.")

    def change_editor(self, user):
        """
        Change the current editor of the Crunch dataset.

        Parameters
        ----------
        :param user:
            The email address, the crunch url or a User instance of the user
            who should be set as the new current editor of the given dataset.

        :returns: None
        """

        def _host_from_url(url):
            resolved = urlsplit(url)
            return resolved.hostname

        def _to_url(email):
            api_users = 'https://{}/api/users/'.format(
                _host_from_url(self.resource.self)
            )
            user_url = None

            users = self.resource.session.get(api_users).payload['index']

            for url, user in six.iteritems(users):
                if user['email'] == email:
                    user_url = url
                    self.resource.patch({'current_editor': url})
                    break
            assert user_url is not None, 'Unable to resolve user url'

            return user_url

        def _is_url(u):
            return u.startswith('https://') or u.startswith('http://')

        if isinstance(user, User):
            user = user.url
        user_url = user if _is_url(user) else _to_url(user)

        self.resource.patch({'current_editor': user_url})

    def stream_rows(self, columns):
        """
        Receives a dict with columns of values to add and streams them
         into the dataset. Client must call .push_rows(n) later or wait until
         Crunch automatically processes the batch.

        Returns the total of rows streamed
        """
        importer = Importer()
        count = len(list(columns.values())[0])
        for x in range(count):
            importer.stream_rows(self.resource,
                                 {a: columns[a][x] for a in columns})
        return count

    def push_rows(self, count):
        """
        Batches in the rows that have been recently streamed. This forces
        the rows to appear in the dataset instead of waiting for crunch
        automatic batcher process.
        """
        self.resource.batches.create({
            'element': 'shoji:entity',
            'body': {
                'stream': count,
                'type': 'ldjson'
            }
        })

    def exclude(self, expr=None):
        """
        Given a dataset object, apply an exclusion filter to it (defined as an
        expression string).

        If the `expr` parameter is None, an empty expression object is sent
        as part of the PATCH request, which effectively removes the exclusion
        filter (if any).

        Exclusion filters express logic that defines a set of rows that should be
        dropped from the dataset. The rows aren't permanently deleted---you can
        recover them at any time by removing the exclusion filter---but they are
        omitted from all views and calculations, as if they had been deleted.

        Note that exclusion filters work opposite from how "normal" filters work.
        That is, a regular filter expression defines the subset of rows to operate
        on: it says "keep these rows." An exclusion filter defines which rows to
        omit. Applying a filter expression as a query filter will have the
        opposite effect if applied as an exclusion. Indeed, applying it as both
        query filter and exclusion at the same time will result in 0 rows.
        """
        if isinstance(expr, six.string_types):
            expr_obj = parse_expr(expr)
            expr_obj = process_expr(expr_obj, self.resource)  # cause we need URLs
        elif expr is None:
            expr_obj = {}
        else:
            expr_obj = expr
        return self.resource.session.patch(
            self.resource.fragments.exclusion,
            data=json.dumps(dict(expression=expr_obj))
        )

    def get_exclusion(self):
        # make sure there is an expression
        expr = self.resource.exclusion.get('body').get('expression')
        if not expr:
            return None
        return prettify(self.resource.exclusion.body.expression, self)

    def add_filter(self, name, expr, public=False):
        payload = dict(element='shoji:entity',
                       body=dict(name=name,
                                 expression=process_expr(parse_expr(expr), self.resource),
                                 is_public=public))
        new_filter = self.resource.filters.create(payload)
        return self.filters[new_filter.body['name']]

    def create_single_response(self, categories, name, alias, description='',
                               missing=True, notes=''):
        """
        Creates a categorical variable deriving from other variables.
        Uses Crunch's `case` function.
        """
        cases = []
        for cat in categories:
            cases.append(cat.pop('case'))

        if not hasattr(self.resource, 'variables'):
            self.resource.refresh()

        args = [{
            'column': [c['id'] for c in categories],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': categories}}}]

        for cat in args[0]['type']['value']['categories']:
            cat.setdefault('missing', False)

        if missing:
            args[0]['column'].append(-1)
            args[0]['type']['value']['categories'].append(dict(
                id=-1,
                name='No Data',
                numeric_value=None,
                missing=True))

        more_args = []
        for case in cases:
            more_args.append(parse_expr(case))

        more_args = process_expr(more_args, self.resource)

        expr = dict(function='case', args=args + more_args)

        payload = dict(element='shoji:entity',
                       body=dict(alias=alias,
                                 name=name,
                                 expr=expr,
                                 description=description,
                                 notes=notes))

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return the variable instance
        return self[new_var['body']['alias']]

    def create_multiple_response(self, responses, name, alias, description='',
                                 notes=''):
        """
        Creates a Multiple response (array) using a set of rules for each
         of the responses(subvariables).
        """
        responses_map = collections.OrderedDict()
        for resp in responses:
            case = resp['case']
            if isinstance(case, six.string_types):
                case = process_expr(parse_expr(case), self.resource)
            responses_map['%04d' % resp['id']] = case_expr(case, name=resp['name'],
                                                           alias='%s_%d' % (alias, resp['id']))

        payload = {
            'element': 'shoji:entity',
            'body': {
                'name': name,
                'alias': alias,
                'description': description,
                'notes': notes,
                'derivation': {
                    'function': 'array',
                    'args': [{
                        'function': 'select',
                        'args': [{
                            'map': responses_map
                        }]
                    }]
                }
            }
        }

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return an instance of Variable
        return self[new_var['body']['alias']]

    def copy_variable(self, variable, name, alias):
        SUBVAR_ALIAS = re.compile(r'.+_(\d+)$')

        def subrefs(_variable, _alias):
            # In the case of MR variables, we want the copies' subvariables
            # to have their aliases in the same pattern and order that the
            # parent's are, that is `parent_alias_#`.
            _subreferences = []
            for subvar in _variable.resource.subvariables.index.values():
                sv_alias = subvar['alias']
                match = SUBVAR_ALIAS.match(sv_alias)
                if match:  # Does this var have the subvar pattern?
                    suffix = int(match.groups()[0], 10)  # Keep the position
                    sv_alias = subvar_alias(_alias, suffix)

                _subreferences.append({
                    'name': subvar['name'],
                    'alias': sv_alias
                })
            return _subreferences

        if variable.resource.body.get('derivation'):
            # We are dealing with a derived variable, we want the derivation
            # to be executed again instead of doing a `copy_variable`
            derivation = abs_url(variable.resource.body['derivation'],
                                 variable.resource.self)
            derivation.pop('references', None)
            payload = {
                'element': 'shoji:entity',
                'body': {
                    'name': name,
                    'alias': alias,
                    'derivation': derivation
                }
            }

            if variable.type == _MR_TYPE:
                # We are re-executing a multiple_response derivation.
                # We need to update the complex `array` function expression
                # to contain the new suffixed aliases. Given that the map is
                # unordered, we have to iterated and find a name match.
                subvars = payload['body']['derivation']['args'][0]['args'][0]['map']
                subreferences = subrefs(variable, alias)
                for subref in subreferences:
                    for subvar_pos in subvars:
                        subvar = subvars[subvar_pos]
                        if subvar['references']['name'] == subref['name']:
                            subvar['references']['alias'] = subref['alias']
                            break
        else:
            payload = {
                'element': 'shoji:entity',
                'body': {
                    'name': name,
                    'alias': alias,
                    'derivation': {
                        'function': 'copy_variable',
                        'args': [{
                            'variable': variable.resource.self
                        }]
                    }
                }
            }
            if variable.type == _MR_TYPE:
                subreferences = subrefs(variable, alias)
                payload['body']['derivation']['references'] = {
                    'subreferences': subreferences
                }

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return an instance of Variable
        return self[new_var['body']['alias']]

    def combine_categories(self, variable, map, categories, missing=None, default=None,
                           name='', alias='', description=''):
        if not alias or not name:
            raise ValueError("Name and alias are required")
        if variable.type in _MR_TYPE:
            return self.combine_multiple_response(variable, map, categories, name=name,
                                                  alias=alias, description=description)
        else:
            return self.combine_categorical(variable, map, categories, missing, default,
                                            name=name, alias=alias, description=description)

    def combine_categorical(self, variable, map, categories=None, missing=None,
                            default=None, name='', alias='', description=''):
        """
        Create a new variable in the given dataset that is a recode
        of an existing variable
            map={
                1: (1, 2),
                2: 3,
                3: (4, 5)
            },
            default=9,
            missing=[-1, 9],
            categories={
                1: "low",
                2: "medium",
                3: "high",
                9: "no answer"
            },
            missing=9
        """
        if isinstance(variable, six.string_types):
            variable = self[variable]

        # TODO: Implement `default` parameter in Crunch API
        combinations = combinations_from_map(map, categories or {}, missing or [])
        payload = _VARIABLE_PAYLOAD_TMPL.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['derivation'] = combine_categories_expr(
            variable.resource.self, combinations)

        # this returns an entity
        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # at this point we are returning a Variable instance
        return self[new_var['body']['alias']]

    def combine_multiple_response(self, variable, map, categories=None, default=None,
                                  name='', alias='', description=''):
        """
        Creates a new variable in the given dataset that combines existing
        responses into new categorized ones

            map={
                1: 1,
                2: [2, 3, 4]
            },
            categories={
                1: "online",
                2: "notonline"
            }

        """
        if isinstance(variable, six.string_types):
            parent_alias = variable
            variable = self[variable]
        else:
            parent_alias = variable.alias

        # TODO: Implement `default` parameter in Crunch API
        responses = responses_from_map(variable, map, categories or {}, alias,
                                       parent_alias)
        payload = _VARIABLE_PAYLOAD_TMPL.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['derivation'] = combine_responses_expr(
            variable.resource.self, responses)

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return an instance of Variable
        return self[new_var['body']['alias']]

    def create_savepoint(self, description):
        """
        Creates a savepoint on the dataset.

        :param description:
            The description that should be given to the new savepoint. This
            function will not let you create a new savepoint with the same
            description as any other savepoint.

        :returns: None
        """
        if len(self.resource.savepoints.index) > 0:
            if description in self.savepoint_attributes('description'):
                raise KeyError(
                    "A checkpoint with the description '{}' already"
                    " exists.".format(description)
                )

        self.resource.savepoints.create({
            'element': 'shoji:entity',
            'body': {'description': description}
        })

    def load_savepoint(self, description=None):
        """
        Load a savepoint on the dataset.

        :param description: default=None
            The description that identifies which savepoint to be loaded.
            When loading a savepoint, all savepoints that were saved after
            the loaded savepoint will be destroyed permanently.

        :returns: None
        """

        if description is None:
            description = 'initial import'
        elif description not in self.savepoint_attributes('description'):
            raise KeyError(
                "No checkpoint with the description '{}'"
                " exists.".format(description)
            )

        revert = self.resource.savepoints.by('description').get(description).revert
        self.resource.session.post(revert)

    def savepoint_attributes(self, attrib):
        """
        Return list of attributes from the given dataset's savepoints.

        :param attrib:
            The attribute to be returned for each savepoint in the given
            dataset. Available attributes are:
                'creation_time'
                'description'
                'last_update'
                'revert'
                'user_name'
                'version'
        """

        if len(self.resource.savepoints.index) == 0:
            return []
        else:
            attribs = [
                cp[attrib]
                for url, cp in six.iteritems(self.resource.savepoints.index)
            ]

            return attribs

    def fork(self, description=None, name=None, is_published=False,
             preserve_owner=False, **kwargs):
        """
        Create a fork of ds and add virgin savepoint.

        :param description: str, default=None
            If given, the description to be applied to the fork. If not
            given the description will be copied from ds.
        :param name: str, default=None
            If given, the name to be applied to the fork. If not given a
            default name will be created which numbers the fork based on
            how many other forks there are on ds.
        :param is_published: bool, default=False
            If True, the fork will be visible to viewers of ds. If False it
            will only be viewable to editors of ds.
        :param preserve_owner: bool, default=False
            If True, the owner of the fork will be the same as the parent
            dataset. If the owner of the parent dataset is a Crunch project,
            then it will be preserved regardless of this parameter.

        :returns _fork: scrunch.datasets.Dataset
            The forked dataset.
        """

        nforks = len(self.resource.forks.index)
        if name is None:
            name = "FORK #{} of {}".format(nforks + 1, self.resource.body.name)
        if description is None:
            description = self.resource.body.description

        body = dict(
            name=name,
            description=description,
            is_published=is_published,
            **kwargs
        )
        # not returning a dataset
        _fork = self.resource.forks.create({"body": body}).refresh()
        fork = Dataset(_fork)

        if preserve_owner or '/api/projects/' in self.resource.body.owner:
            try:
                _fork.patch({'owner': self.resource.body.owner})
                _fork.refresh()
            except (pycrunch.ClientError, pycrunch.ServerError):
                # Don't fail if the fork's owner cannot be set to be the same
                # as the parent dataset.
                pass

        return fork

    def merge(self, fork_id=None, autorollback=True):
        """
        :param fork_id: str or int
            can either be the fork id, name or its number as string or int

        :param autorollback: bool, default=True
            if True the original dataset is rolled back to the previous state
            in case of a merge conflict.
            if False the dataset and fork are beeing left 'dirty'
        """
        if isinstance(fork_id, int) or (
                isinstance(fork_id, six.string_types) and
                fork_id.isdigit()):
            fork_id = "FORK #{} of {}".format(fork_id, self.resource.body.name)

        elif fork_id is None:
            raise ValueError('fork id, name or number missing')

        fork_index = self.resource.forks.index

        forks = [f for f in fork_index
                 if fork_index[f].get('name') == fork_id or
                 fork_index[f].get('id') == fork_id]
        if len(forks) == 1:
            fork_url = forks[0]
        else:
            raise ValueError(
                "Couldn't find a (unique) fork. "
                "Please try again using its id")

        body = dict(
            dataset=fork_url,
            autorollback=autorollback
        )
        self.resource.actions.post(body)

    def forks_dataframe(self):
        """
        Return a dataframe summarizing the forks on the dataset.

        :returns _forks : pandas.DataFrame
            A DataFrame representation of all attributes from all forks
            on the given dataset.
        """

        if len(self.resource.forks.index) == 0:
            return None
        else:
            _forks = pd.DataFrame(
                [fk for url, fk in six.iteritems(self.resource.forks.index)]
            )
            _forks = _forks[[
                'name',
                'description',
                'is_published',
                'owner_name',
                'current_editor_name',
                'creation_time',
                'modification_time',
                'id'
            ]]
            _forks['creation_time'] = pd.to_datetime(_forks['creation_time'])
            _forks['modification_time'] = pd.to_datetime(_forks['modification_time'])
            _forks.sort(columns='creation_time', inplace=True)

            return _forks

    def delete_forks(self):
        """
        Deletes all the forks on the dataset. CANNOT BE UNDONE!
        """

        for fork in six.itervalues(self.resource.forks.index):
            fork.entity.delete()

    def export(self, path, format='csv', filter=None, variables=None,
               hidden=False, options=None, metadata_path=None):
        """
        Downloads a dataset as CSV or as SPSS to the given path. This
        includes hidden variables.

        Dataset viewers can't download hidden variables so we default
        to False. Dataset editors will need to add hidden=True if they
        need this feature.

        By default, categories in CSV exports are provided as id's.
        """
        valid_options = ['use_category_ids', 'prefix_subvariables',
                         'var_label_field', 'missing_values']

        # Only CSV and SPSS exports are currently supported.
        if format not in ('csv', 'spss'):
            raise ValueError(
                'Invalid format %s. Allowed formats are: "csv" and "spss".'
                % format
            )

        if format == 'csv':
            # Default options for CSV exports.
            export_options = {'use_category_ids': True}
        else:
            # Default options for SPSS exports.
            export_options = {
                'prefix_subvariables': False,
                'var_label_field': 'description'
            }

        # Validate the user-provided export options.
        options = options or {}
        if not isinstance(options, dict):
            raise ValueError(
                'The options argument must be a dictionary.'
            )

        for k in options.keys():
            if k not in valid_options:
                raise ValueError(
                    'Invalid options for format "%s": %s.'
                    % (format, ','.join(k))
                )
        if 'var_label_field' in options \
                and not options['var_label_field'] in ('name', 'description'):
            raise ValueError(
                'The "var_label_field" export option must be either "name" '
                'or "description".'
            )

        # All good. Update the export options with the user-provided values.
        export_options.update(options)

        # the payload should include all hidden variables by default
        payload = {'options': export_options}

        # Option for exporting metadata as json
        if metadata_path is not None:
            metadata = self.resource.table['metadata']
            if variables is not None:
                if sys.version_info >= (3, 0):
                    metadata = {
                        key: value
                        for key, value in metadata.items()
                        if value['alias'] in variables
                    }
                else:
                    metadata = {
                        key: value
                        for key, value in metadata.iteritems()
                        if value['alias'] in variables
                    }
            with open(metadata_path, 'w+') as f:
                json.dump(metadata, f, sort_keys=True)

        # add filter to rows if passed
        if filter:
            if isinstance(filter, Filter):
                payload['filter'] = {'filter': filter.resource.self}
            else:
                payload['filter'] = process_expr(
                    parse_expr(filter), self.resource
                )

        # convert variable list to crunch identifiers
        if variables and isinstance(variables, list):
            id_vars = []
            for var in variables:
                id_vars.append(self[var].url)
            if len(id_vars) != len(variables):
                LOG.debug(
                    "Variables passed: %s Variables detected: %s"
                    % (variables, id_vars)
                )
                raise AttributeError("At least a variable was not found")
            # Now build the payload with selected variables
            payload['where'] = {
                'function': 'select',
                'args': [{
                    'map': {
                        x: {'variable': x} for x in id_vars
                    }
                }]
            }
        # hidden is mutually exclusive with
        # variables to include in the download
        if hidden and not variables:
            if not self.resource.body.permissions.edit:
                raise AttributeError("Only Dataset editors can export hidden variables")
            payload['where'] = {
                'function': 'select',
                'args': [{
                    'map': {
                        x: {'variable': x}
                        for x in self.resource.variables.index.keys()
                    }
                }]
            }

        url = export_dataset(self.resource, payload, format=format)
        download_file(url, path)

    def join(self, left_var, right_ds, right_var, columns=None,
             filter=None, wait=True):
        """
        Joins a given variable. In crunch joins are left joins, where
        left is the dataset variable and right is other dataset variable.
        For more information see:
        http://docs.crunch.io/?http#merging-and-joining-datasets

        :param: columns: Specify a list of variables from right dataset
        to bring in the merge:
        http://docs.crunch.io/?http#joining-a-subset-of-variables

        :param: wait: Wait for the join progress to finish by polling
        or simply return a url to the progress resource

        :param: filter: Filters out rows based on the given expression,
        or on a given url for an existing filter. TODO: for the moment
        we only allow expressions
        """
        right_var_url = right_ds[right_var].url
        left_var_url = self[left_var].url
        # this dictionary sets the main part of the join
        adapter = {
            'function': 'adapt',
            'args': [
                {'dataset': right_ds.url},
                {'variable': right_var_url},
                {'variable': left_var_url}
            ]
        }

        # wrap the adapter method on a shoji and body entity
        payload = {
            'element': 'shoji:entity',
            'body': adapter
        }

        if columns and isinstance(columns, list):
            # overwrite body to new format
            payload['body'] = {
                'frame': adapter,
                'function': 'select',
                'args': [
                    {'map': {}}
                ]
            }
            # add the individual variable columns to the payload
            for var in columns:
                var_url = right_ds[var].url
                payload['body']['args'][0]['map'][var_url] = {'variable': var_url}

        if filter:
            # in the case of a filter, convert it to crunch
            # and attach the filter to the payload
            expr = process_expr(parse_expr(filter), right_ds)
            payload['body']['filter'] = {'expression': expr}

        progress = self.resource.variables.post(payload)
        # poll for progress to finish or return the url to progress
        if wait:
            return wait_progress(r=progress, session=self.resource.session, entity=self)
        return progress.json()['value']

    def create_categorical(self, categories, alias, name, multiple,
                           description='', notes=''):
        """
        Used to create new categorical variables using Crunchs's `case`
        function

        Will create either categorical variables or multiple response depending
        on the `multiple` parameter.
        """
        if multiple:
            return self.create_multiple_response(
                categories, alias=alias, name=name, description=description,
                notes=notes)
        else:
            return self.create_single_response(
                categories, alias=alias, name=name, description=description,
                notes=notes)

    def create_crunchbox(
            self, title=None, header=None, footer=None, notes=None,
            filters=None, variables=None, force=False):
        """
        create a new boxdata entity for a CrunchBox.

        NOTE: new boxdata is only created when there is a new combination of
        where and filter data.

        Args:
            title       (str): Human friendly identifier
            notes       (str): Other information relevent for this CrunchBox
            header      (str): header information for the CrunchBox
            footer      (str): footer information for the CrunchBox
            filters    (list): list of filter names or `Filter` instances
            where      (list): list of variable aliases or `Variable` instances
                               If `None` all variables will be included.
        Returns:
            CrunchBox (instance)
        """

        if filters:
            if not isinstance(filters, list):
                raise TypeError('`filters` argument must be of type `list`')

            # ensure we only have `Filter` instances
            filters = [
                f if isinstance(f, Filter) else self.filters[f]
                for f in filters
            ]

            if any(not f.is_public
                    for f in filters):
                raise ValueError('filters need to be public')

            filters = [
                {'filter': f.resource.self}
                for f in filters
            ]

        if variables:
            if not isinstance(variables, list):
                raise TypeError('`variables` argument must be of type `list`')

            # ensure we only have `Variable` Tuples
            # NOTE: if we want to check if variables are public we would have
            # to use Variable instances instead of their Tuple representation.
            # This would cause additional GET's
            variables = [
                var.shoji_tuple if isinstance(var, Variable)
                else self.resource.variables.by('alias')[var]
                for var in variables
            ]

            variables = dict(
                function='select',
                args=[
                    {'map': {
                        v.id: {'variable': v.entity_url}
                        for v in variables
                    }}
                ])

        if not title:
            title = 'CrunchBox for {}'.format(str(self))

        payload = dict(
            element='shoji:entity',
            body=dict(
                where=variables,
                filters=filters,
                force=force,
                title=title,
                notes=notes,
                header=header,
                footer=footer)
        )

        # create the boxdata
        self.resource.boxdata.create(payload)

        # NOTE: the entity from the response is a bit different compared to
        # others, i.e. no id, no delete method, different entity_url...
        # For now, return the shoji_tuple from the index
        for shoji_tuple in self.resource.boxdata.index.values():
            if shoji_tuple.metadata.title == title:
                return CrunchBox(shoji_tuple, self)

    def delete_crunchbox(self, **kwargs):
        """ deletes crunchboxes on matching kwargs """
        match = False
        for key in kwargs:
            if match:
                break
            for crunchbox in self.crunchboxes:
                attr = getattr(crunchbox, key, None)
                if attr and attr == kwargs[key]:
                    crunchbox.remove()
                    match = True
                    break


class DatasetSubvariablesMixin(DatasetVariablesMixin):
    """
    Handles a variable subvariables iteration in a dict-like way
    """

    def __getitem__(self, item):
        # Check if the attribute corresponds to a variable alias
        subvariable = self.resource.subvariables.by('alias').get(item)
        if subvariable is None:
            # subvariable doesn't exists, must raise a ValueError
            raise KeyError(
                'Variable %s has no subvariable with an alias %s' % (self.name,
                                                                     item))
        # subvariable exists!, return the subvariable Instance
        return Variable(subvariable, self)

    def __iter__(self):
        """
        Iterable of ordered subvariables
        """
        sv_index = self.resource.subvariables.index
        for sv in self.resource['body']['subvariables']:
            yield sv_index[sv]

    def __len__(self):
        return len(self.resource.subvariables.index)

    def itervalues(self):
        for _, var_tuple in self.resource.subvariables.index.items():
            yield Variable(var_tuple, self)

    def iterkeys(self):
        for var in self.resource.subvariables.index.items():
            yield var[1].name


class Variable(ReadOnly, DatasetSubvariablesMixin):
    """
    A pycrunch.shoji.Entity wrapper that provides variable-specific methods.
    """
    _MUTABLE_ATTRIBUTES = {'name', 'description',
                           'view', 'notes', 'format'}
    _IMMUTABLE_ATTRIBUTES = {'id', 'alias', 'type', 'discarded'}
    # We won't expose owner and private
    # categories in immutable. IMO it should be handled separately
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES
    _OVERRIDDEN_ATTRIBUTES = {'categories'}

    CATEGORICAL_TYPES = {'categorical', 'multiple_response', 'categorical_array'}

    def __init__(self, var_tuple, dataset):
        """
        :param var_tuple: A Shoji Tuple for a dataset variable
        :param dataset: a Dataset() object instance
        """
        self.shoji_tuple = var_tuple
        self.is_instance = False
        self._resource = None
        self.url = var_tuple.entity_url
        self.dataset = dataset

    @property
    def resource(self):
        if not self.is_instance:
            self._resource = self.shoji_tuple.entity
            self.is_instance = True
        return self._resource

    def __getattr__(self, item):
        # don't access self.resource unless necessary
        if hasattr(self.shoji_tuple, item):
            return self.shoji_tuple[item]
        if item in self._ENTITY_ATTRIBUTES - self._OVERRIDDEN_ATTRIBUTES:
            try:
                return self.resource.body[item]  # Has to exist
            except KeyError:
                raise AttributeError("Variable does not have attribute %s" % item)
        return super(Variable, self).__getattribute__(item)

    def edit(self, **kwargs):
        for key in kwargs:
            if key not in self._MUTABLE_ATTRIBUTES:
                raise AttributeError("Can't edit attribute %s of variable %s" % (
                    key, self.name
                ))
        return self.resource.edit(**kwargs)

    def __repr__(self):
        return "<Variable: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name

    @property
    def categories(self):
        if self.resource.body['type'] not in self.CATEGORICAL_TYPES:
            raise TypeError("Variable of type %s do not have categories" % self.resource.body.type)
        return CategoryList._from(self.resource)

    def hide(self):
        LOG.debug("HIDING")
        self.resource.edit(discarded=True)

    def unhide(self):
        LOG.debug("UNHIDING")
        self.resource.edit(discarded=False)

    def combine(self, alias=None, map=None, names=None, default='missing',
                name=None, description=None):
        # DEPRECATED - USE Dataset.combine*
        """
        Implements SPSS-like recode functionality for Crunch variables.

        This method combines Crunch's `combine_categories` and
        `combine_responses` in a single method when applied to a variable
        that is deemed as ~categorical~ by the user.
        """
        if alias is None:
            raise TypeError('Missing alias for the recoded variable')
        if map is None:
            raise TypeError('Missing recode map')
        if not isinstance(map, dict) or not map:
            raise ValueError('Invalid recode map')
        if default not in ('missing', 'copy'):
            raise ValueError(
                'The "default" argument must be either "missing" or "copy"'
            )

        if 'body' not in self.resource:
            self.resource.refresh()

        if self.resource.body.type not in ('categorical', 'categorical_array',
                                           _MR_TYPE):
            raise TypeError(
                'Only categorical, categorical_array and multiple_response '
                'variables are supported'
            )

        if name is None:
            name = self.resource.body.name + ' (recoded)'

        if description is None:
            description = self.resource.body.description

        if self.resource.body.type in ('categorical', 'categorical_array'):
            # On this case perform a `combine_categories` operation

            if names is None:
                raise TypeError('Missing category names')

            processed_categories = list()
            category_defs = list()
            category_defs_by_id = dict()
            default_name = 'Missing'
            existing_categories_by_id = dict()
            existing_categories_by_name = dict()
            for existing_category in self.resource.body.get('categories', []):
                _id = existing_category['id']
                _name = existing_category['name']
                existing_categories_by_id[_id] = existing_category
                existing_categories_by_id[_name] = existing_category

            # 1. Basic category definitions.
            for _id, value in map.items():
                if isinstance(_id, int):
                    if not isinstance(value, (list, tuple, int,
                                              six.string_types, range)):
                        raise ValueError('Invalid mapped value')
                    if isinstance(value, (int, six.string_types)):
                        value = [value]

                    processed_value = []
                    for element in value:
                        if isinstance(element, six.string_types):
                            try:
                                element = existing_categories_by_name[element]
                            except KeyError:
                                raise ValueError(
                                    'Invalid category name %s' % element
                                )
                            assert isinstance(element, int)
                        if isinstance(element, int):
                            if element not in existing_categories_by_id:
                                raise ValueError(
                                    'Invalid numeric code %s' % element
                                )
                        else:
                            raise ValueError(
                                'Invalid mapped value %s' % element
                            )
                        processed_value.append(element)

                    category = {
                        'id': _id,
                        'name': str(_id),
                        'missing': False,
                        'combined_ids': processed_value
                    }
                    category_defs.append(category)
                    category_defs_by_id[_id] = category
                    processed_categories.extend(list(value))
            category_defs = sorted(category_defs, key=lambda c: c['id'])

            # 2. Add category names (if defined).
            if type(names) in (list, tuple):
                for i, category_name in enumerate(names):
                    if i < len(category_defs):
                        category_defs[i]['name'] = category_name
                    elif i == len(category_defs):
                        default_name = category_name
            elif isinstance(names, dict):
                for _id, category_name in names.items():
                    if _id in category_defs_by_id:
                        category_defs_by_id[_id]['name'] = category_name

            # 3. Add the "missing" stuff.
            missing_category = {
                'id': max(category_defs_by_id.keys()) + 1,
                'name': default_name,
                'missing': True,
                'combined_ids': []
            }
            for existing_category in self.resource.body.get('categories', []):
                _id = existing_category['id']
                if _id not in processed_categories:
                    if default == 'missing':
                        missing_category['combined_ids'].append(_id)
                    elif default == 'copy':
                        category = {
                            'id': _id,
                            'name': existing_category['name'],
                            'missing': existing_category['missing'],
                            'combined_ids': [_id]
                        }
                        category_defs.append(category)
                        category_defs_by_id[_id] = category
                    processed_categories.append(_id)

            if default == 'missing':
                category_defs.append(missing_category)

            # 4. Create the recoded variable.
            payload = _VARIABLE_PAYLOAD_TMPL.copy()
            payload['body']['name'] = name
            payload['body']['alias'] = alias
            payload['body']['description'] = description
            payload['body']['expr']['function'] = 'combine_categories'
            payload['body']['expr']['args'] = [
                {
                    'variable': self.resource['self']
                },
                {
                    'value': category_defs
                }
            ]
        else:  # multiple_response
            # Perform a `combine_responses` derivation
            subreferences = self.resource.body.get('subreferences', [])
            subvariables = self.resource.body.get('subvariables', [])
            assert len(subreferences) == len(subvariables)

            # 1. Gather the URLs of the subvariables.
            subvar_urls = {
                subvar['alias']: subvariables[i]
                for i, subvar in enumerate(subreferences)
            }

            # 2. Generate the list of response definitions for the recoded
            #    variable.
            response_defs = list()
            for new_subvar_name in sorted(map):
                value = map[new_subvar_name]
                if not isinstance(value, (list, tuple, six.string_types)):
                    raise ValueError
                if isinstance(value, six.string_types):
                    value = [value]
                try:
                    response = {
                        'name': new_subvar_name,
                        'combined_ids': [
                            subvar_urls[_alias] for _alias in value
                        ]
                    }
                    response_defs.append(response)
                except KeyError:
                    raise ValueError(
                        'Invalid subvariable alias(es) in %s' % value
                    )

            # 3. Create the recoded variable.
            payload = _VARIABLE_PAYLOAD_TMPL.copy()
            payload['body']['name'] = name
            payload['body']['alias'] = alias
            payload['body']['description'] = description
            payload['body']['expr']['function'] = 'combine_responses'
            payload['body']['expr']['args'] = [
                {
                    'variable': self.resource['self']
                },
                {
                    'value': response_defs
                }
            ]

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self.dataset._reload_variables()
        # return an instance of Variable
        return self.dataset[new_var['body']['alias']]

    def edit_categorical(self, categories, rules):
        # validate rules and categories are same size
        _validate_category_rules(categories, rules)
        args = [{
            'column': [c['id'] for c in categories],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': categories}}}]
        # build the expression
        more_args = []
        for rule in rules:
            more_args.append(parse_expr(rule))
        # get dataset and build the expression
        more_args = process_expr(more_args, self.dataset)
        # epression value building
        expr = dict(function='case', args=args + more_args)
        payload = dict(
            element='shoji:entity',
            body=dict(expr=expr)
        )
        # patch the variable with the new payload
        return self.resource.patch(payload)

    def edit_derived(self, variable, mapper):
        raise NotImplementedError("Use edit_combination")

    def move(self, path, position=-1):
        path = Path(path)
        if not path.is_absolute:
            raise InvalidPathError(
                'Invalid path %s: only absolute paths are allowed.' % path
            )
        target_group = self.dataset.order[str(path)]
        target_group.insert(self.alias, position=position)
