import abc
import collections
import json
import os

import pycrunch
import requests
import six

from pycrunch.exporting import export_dataset
from pycrunch.importing import Importer
from pycrunch.shoji import Entity, wait_progress
from scrunch.expressions import parse_expr, process_expr
from scrunch.variables import validate_variable_url

import pandas as pd

if six.PY2:  # pragma: no cover
    from urlparse import urlsplit
    import ConfigParser as configparser
else:
    from urllib.parse import urlsplit
    import configparser


SKELETON = {
    'element': 'shoji:entity',
    'body': {
        'name': 'name',
        'description': 'description',
        'alias': 'alias',
        'expr': {
            'function': 'function',
            'args': []
        }
    }
}

REQUIRED_VALUES = {'name', 'id', 'missing', 'combined_ids'}
REQUIRES_RESPONSES = {'combined_ids', 'name'}


def _get_site():
    """
    Utilitarian function that reads credentials from
    file or from ENV variables
    """
    # try to get credentials from enviroment
    username = os.environ.get('CRUNCH_USERNAME')
    password = os.environ.get('CRUNCH_PASSWORD')
    site = os.environ.get('CRUNCH_URL')
    if username and password and site:
        print("Found Crunch credentials on Environment")
        return pycrunch.connect(username, password, site)
    elif username and password:
        print("Found Crunch credentials on Environment")
        return pycrunch.connect(username, password)
    # try reading from .ini file
    config = configparser.ConfigParser()
    config.read('crunch.ini')
    username = config['DEFAULT'].get('CRUNCH_USERNAME')
    password = config['DEFAULT'].get('CRUNCH_PASSWORD')
    site = config['DEFAULT'].get('CRUNCH_URL')
    # now try to login with obtained creds
    if username and password and site:
        print("Found Crunch credentials on crunch.ini")
        return pycrunch.connect(username, password, site)
    elif username and password:
        print("Found Crunch credentials on crunch.ini")
        return pycrunch.connect(username, password)
    else:
        raise AttributeError('No crunch.ini file found and no '
                             'environment variables found')


def get_dataset(dataset, site=None):
    """
    Retrieve a reference to a given dataset (either by name, or ID) if
    it exists. This method uses the library singleton session if the
    optional "site" parameter is not provided.

    Returns a Dataset Entity record if the dataset exists.
    Raises a KeyError if no such dataset exists.
    """
    if site is None:
        if pycrunch.session is None:
            site = _get_site()
            if not site:
                raise AttributeError(
                    "Authenticate first with scrunch.connect() or"
                    "providing environment variables")
        else:
            site = pycrunch.session
    try:
        shoji_ds = site.datasets.by('name')[dataset].entity
    except KeyError:
        shoji_ds = site.datasets.by('id')[dataset].entity
    return Dataset(shoji_ds)


def change_project(project, site=None):
    """
    :param project: name or ID of the project
    :param site: scrunch session, defaults to global session
    :return: the project session
    """
    if site is None:
        if pycrunch.session is None:
            raise AttributeError("Authenticate first with scrunch.connect()")
        site = pycrunch.session
    try:
        ret = site.projects.by('name')[project].entity
    except KeyError:
        ret = site.projects.by('id')[project].entity
    pycrunch.session = ret


def create_dataset(name, variables, site=None):
    if site is None:
        if pycrunch.session is None:
            raise AttributeError("Authenticate first with scrunch.connect()")
        site = pycrunch.session

    shoji_ds = site.datasets.create({
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


def var_name_to_url(ds, alias):
    """
    :param ds: The dataset we are gonna inspect
    :param alias: the alias of the variable name we want to check
    :return: the id of the given varname or None
    """
    try:
        return ds.variables.by('alias')[alias].entity.self
    except KeyError:
        raise KeyError(
            'Variable %s does not exist in Dataset %s' % (alias,
                                                          ds['body']['name']))


def var_id_to_url(ds, id):
    """
    :param ds: The dataset to look for the id of variable
    :param id: The id string of a variable
    :return: the url of the given variable as crunch url
    """
    try:
        return ds.variables.by('id')[id].entity.self
    except KeyError:
        raise KeyError(
            'Variable %s does not exist in Dataset %s' % (id,
                                                          ds['body']['name']))


def variable_to_url(ds, variable):
    """Receive a valid variable reference and return the variable url.

    :param ds: The crunch dataset
    :param variable: A valid variable reference in the form of a shoji Entity
                     of the variable or a string containing the variable url
                     or alias.
    :return: The variable url
    """
    assert isinstance(variable, (six.string_types, Entity))

    if isinstance(variable, Entity):
        return variable.self

    elif validate_variable_url(variable):
        return variable
    else:
        try:
            return var_name_to_url(ds, variable)
        except KeyError:
            return var_id_to_url(ds, variable)


def aliases_to_urls(ds, variable_url, response_map):
    """
    Maps subvariable aliases to urls
    :param ds: a dataset object
    :param variable_url: url of the variable we want to inspect
    :param response_map: mapping of new subvariables
    :return:
    """
    suvars = ds.session.get(variable_url).payload.subvariables.by('alias')
    mapped_urls = {}
    for key, values in response_map.items():
        try:
            mapped_urls[key] = [suvars[x].entity.self for x in values]
        except KeyError:
            raise KeyError(
                'Unexistant variables %s in Dataset %s' % (
                    values, ds['body']['alias']))
    return mapped_urls


def validate_category_rules(categories, rules):
    if not ((len(categories) - 1) <= len(rules) <= len(categories)):
        raise ValueError(
            'Amount of rules should match categories (or categories -1)'
        )


def validate_category_map(map):
    """
    :param map: categories keyed by new category id mapped to existing ones
    :return: a list of dictionary objects that the Crunch API expects
    """
    for value in map.values():
        keys = set(list(value.keys()))
        assert keys & REQUIRED_VALUES, (
            'category_map has one or more missing keys of ' % REQUIRED_VALUES)
    rebuilt = list()
    for key, value in map.items():
        category = dict()
        category.update(value)
        # unfold expressions like range(1,5) to a list of ids
        category['combined_ids'] = list(category['combined_ids'])
        category['id'] = key
        rebuilt.append(category)
    return rebuilt


def validate_response_map(map):
    """
    :param map: responses keyed by new alias mapped to existing aliases
    :return: a list of dictionaries describing the new responses to create for
             the variable
    """
    rebuilt = list()
    for key, value in map.items():
        response = dict()
        response['name'] = key
        response['combined_ids'] = value
        rebuilt.append(response)
    return rebuilt


def download_file(url, filename):
    if url.startswith('file://'):
        # Result is in local filesystem (for local development mostly)
        import shutil
        shutil.copyfile(url.split('file://', 1)[1], filename)
    else:
        r = requests.get(url, stream=True)
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:   # filter out keep-alive new chunks
                    f.write(chunk)
    return filename


class OrderUpdateError(Exception):
    pass


class AbstractContainer(object):
    __metaclass__ = abc.ABCMeta

    indent_size = 4

    def __getitem__(self, item):
        if not isinstance(item, (int, six.string_types)):
            raise TypeError('arg 1 must be either int or str')
        if isinstance(item, int):
            key, obj = list(self.elements.items())[item]
        else:
            key = item
            obj = self.elements[item]
        if isinstance(obj, six.string_types):  # it's a variable ID
            # Swap the variable ID for a Variable object.
            var = Variable(resource=self.order.vars[obj].entity)
            obj = self.elements[key] = var
        return obj

    def __contains__(self, item):
        return item in self.elements


class Hierarchy(AbstractContainer):
    def __init__(self, group):
        self.group = group
        self.elements = group.elements
        self.order = group.order

    def __str__(self):
        def _get_elements(group):
            elements = []
            for name, obj in list(group.elements.items()):
                if isinstance(obj, Group):
                    elements.append({name: _get_elements(obj)})
                else:
                    elements.append(name)
            return elements

        str_elements = _get_elements(self.group)
        return json.dumps(str_elements, indent=self.indent_size)

    def __repr__(self):
        return self.__str__()


class VariableList(AbstractContainer):
    def __init__(self, group):
        self.group = group
        self.order = group.order

    @property
    def elements(self):
        class ElementsWrapper(collections.OrderedDict):
            def __init__(self, container, *args, **kwargs):
                self.order = container.order
                super(ElementsWrapper, self).__init__(*args, **kwargs)

            def __setitem__(self, key, value, *args):
                super(ElementsWrapper, self).__setitem__(key, value, *args)
                if isinstance(value, Variable):
                    group = self.order.find(key)
                    if group and group.elements[key] != value:
                        group.elements[key] = value

        flattened_elements = ElementsWrapper(self)

        def _get_elements(group):
            for name, obj in list(group.elements.items()):
                if isinstance(obj, Group):
                    _get_elements(obj)
                else:
                    flattened_elements[name] = obj

        _get_elements(self.group)

        return flattened_elements

    def __str__(self):
        return json.dumps(list(self.elements.keys()), indent=self.indent_size)

    def __repr__(self):
        return self.__str__()


class Group(AbstractContainer):

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
                    self.elements[var['alias']] = _id
            elif isinstance(element, Variable):
                self.elements[element.alias] = element
            else:
                subgroup = Group(element, order=self.order, parent=self)
                self.elements[subgroup.name] = subgroup

        self.hierarchy = Hierarchy(self)
        self.variables = VariableList(self)

    def __str__(self):
        str_elements = []
        for alias, obj in self.elements.items():
            if isinstance(obj, six.string_types) or isinstance(obj, Variable):
                str_elements.append(alias)
            else:
                str_elements.append('Group(%s)' % obj.name)
        return json.dumps(str_elements, indent=self.indent_size)

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def _validate_elements_arg(elements):
        if isinstance(elements, six.string_types):
            elements = [elements]
        if not isinstance(elements, collections.Iterable):
            raise ValueError(
                'Invalid list of elements to be inserted into the Group.'
            )
        if not all(isinstance(e, six.string_types) for e in elements):
            raise ValueError(
                'Only string references to aliases/group names are allowed.'
            )
        return elements

    def _validate_reference_arg(self, reference):
        if not isinstance(reference, six.string_types):
            raise TypeError('Invalid reference. It must be a string.')
        if reference not in self.elements:
            raise ValueError(
                'Invalid reference. It is not part of the current Group.'
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

    def move(self, elements, position=-1):  # noqa: C901
        elements = self._validate_elements_arg(elements)

        if not isinstance(position, int):
            raise ValueError('Invalid position. It must be an integer.')
        if position < -1 or position > len(self.elements):
            raise ValueError('Invalid position %d' % position)
        if position == -1:
            position = len(self.elements)

        elements_to_move = collections.OrderedDict()
        for element_name in elements:
            if element_name in self.elements:
                elements_to_move[element_name] = \
                    (self.elements[element_name], '__move__')
            else:
                current_group = self.order.find(element_name)
                if current_group:
                    # A variable.
                    elements_to_move[element_name] = \
                        (current_group, '__migrate_var__')
                else:
                    # Not a variable. A group, maybe?
                    group_to_move = self.order.find_group(element_name)
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

    def move_before(self, reference, elements):
        reference = self._validate_reference_arg(reference)
        elements = self._validate_elements_arg(elements)

        position = 0
        i = 0
        for name in self.elements.keys():
            if name in elements:
                continue
            if reference == name:
                position = i
                break
            i += 1

        self.move(elements, position=position)

    def move_after(self, reference, elements):
        reference = self._validate_reference_arg(reference)
        elements = self._validate_elements_arg(elements)

        position = 0
        i = 0
        for name in self.elements.keys():
            if name in elements:
                continue
            if reference == name:
                position = i + 1
                break
            i += 1

        self.move(elements, position=position)

    def move_up(self, element):
        element = self._validate_reference_arg(element)

        position = 0
        for i, name in enumerate(self.elements.keys()):
            if name == element:
                position = i - 1
                break

        if position == -1:
            # Nothing to do.
            return

        self.move(element, position=position)

    def move_down(self, element):
        element = self._validate_reference_arg(element)

        position = 0
        for i, name in enumerate(self.elements.keys()):
            if name == element:
                position = i + 1
                break

        if position == len(self.elements):
            # Nothing to do.
            return

        self.move(element, position=position)

    def move_top(self, element):
        self.move(element, position=0)

    def move_bottom(self, element):
        self.move(element, position=-1)

    def set(self, elements):
        existing_elements = [name for name in self.elements.keys()]
        if len(elements) != len(existing_elements) or \
                not all(e in existing_elements for e in elements):
            raise ValueError('Invalid list of element references.')

        if elements == existing_elements:
            # Nothing to do.
            return

        _elements = collections.OrderedDict()
        for element in elements:
            _elements[element] = self.elements[element]
        self.elements = _elements

        self.order.update()

    def create(self, name, elements=None):
        if name in self.elements:
            raise ValueError(
                'A variable/sub-group named \'%s\' already exists.' % name
            )
        if elements is None:
            elements = []
        else:
            elements = self._validate_elements_arg(elements)

        # Locate all elements to move. All of them have to exist.
        elements_to_move = collections.OrderedDict()
        for element_name in elements:
            current_group = self.order.find(element_name)
            if current_group and element_name in current_group.elements:
                elements_to_move[element_name] = (current_group,)
            else:
                group_to_move = self.order.find_group(element_name)
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
        if self.name == '__root__' and self.parent is None:
            raise NotImplementedError(
                'Renaming the root Group is not allowed.'
            )
        if name == self.name:
            # Nothing to do.
            return

        if name in self.parent.elements:
            raise ValueError(
                'Parent Group \'%s\' already contains an element named \'%s\'.'
                % (self.parent.name, name)
            )

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

    def remove(self, elements):
        if self.name == '__root__' and self.parent is None:
            raise NotImplementedError(
                'Removing elements from the root Group is not allowed.'
            )
        if isinstance(elements, six.string_types):
            elements = [elements]
        if not isinstance(elements, collections.Iterable):
            raise ValueError(
                'Invalid list of elements to remove from Group.'
            )
        if not all(isinstance(e, six.string_types) for e in elements):
            raise ValueError(
                'Only string references to aliases/group names are allowed.'
            )

        # Locate all elements to remove. All of them have to be found.
        elements_to_remove = collections.OrderedDict()
        for element_name in elements:
            if element_name not in self.elements:
                raise ValueError(
                    'A variable/sub-group named \'%s\' does not exist '
                    'within the Group.' % element_name
                )
            elements_to_remove[element_name] = self.elements[element_name]

        # Make the modifications to the order structure.
        for element_name, obj in elements_to_remove.items():
            if isinstance(obj, Group):
                obj.parent = self.order.graph
            self.order.graph.elements[element_name] = obj
            del self.elements[element_name]

        # Update!
        self.order.update()

    def delete(self):
        if self.name == '__root__' and self.parent is None:
            raise NotImplementedError(
                'Deleting the root Group is not allowed.')

        # Before deleting the Group, move all its elements to the root.
        elements = self.elements.copy()
        for element_name, obj in elements.items():
            if isinstance(obj, Group):
                obj.parent = self.order.graph
            self.order.graph.elements[element_name] = obj
            del self.elements[element_name]

        # Delete from parent.
        del self.parent.elements[self.name]

        # Update!
        self.order.update()


class Order(object):

    def __init__(self, ds):
        self.ds = ds
        self._hier = None
        self._vars = None
        self._graph = None

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

    @property
    def vars(self):
        if self._vars is None:
            self._load_vars()
        return self._vars

    @property
    def graph(self):
        if self._graph is None:
            self._load_graph()
        return self._graph

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
                        _id = obj
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

    @property
    def hierarchy(self):
        return self.graph.hierarchy

    @property
    def variables(self):
        return self.graph.variables

    def __str__(self):
        return str(self.graph)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, item):
        return self.graph[item]

    def __contains__(self, item):
        return item in self.graph

    def find(self, *args, **kwargs):
        return self.graph.find(*args, **kwargs)

    def find_group(self, *args, **kwargs):
        return self.graph.find_group(*args, **kwargs)

    def move(self, *args, **kwargs):
        self.graph.move(*args, **kwargs)

    def move_before(self, *args, **kwargs):
        self.graph.move_before(*args, **kwargs)

    def move_after(self, *args, **kwargs):
        self.graph.move_after(*args, **kwargs)

    def move_up(self, *args, **kwargs):
        self.graph.move_up(*args, **kwargs)

    def move_down(self, *args, **kwargs):
        self.graph.move_down(*args, **kwargs)

    def move_top(self, *args, **kwargs):
        self.graph.move_top(*args, **kwargs)

    def move_bottom(self, *args, **kwargs):
        self.graph.move_bottom(*args, **kwargs)

    def set(self, *args, **kwargs):
        self.graph.set(*args, **kwargs)

    def create(self, *args, **kwargs):
        self.graph.create(*args, **kwargs)

    def rename(self, *args, **kwargs):
        self.graph.rename(*args, **kwargs)

    def remove(self, *args, **kwargs):
        self.graph.remove(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self.graph.delete(*args, **kwargs)


class Dataset(object):
    """
    A pycrunch.shoji.Entity wrapper that provides dataset-specific methods.
    """

    ENTITY_ATTRIBUTES = {'id', 'name', 'notes', 'descrpition', 'is_published',
                         'archived', 'end_date', 'start_date', 'creation_time',
                         'modification_time'}

    def __init__(self, resource):
        """
        :param resource: Points to a pycrunch Shoji Entity for a dataset.
        """
        self.resource = resource
        self.session = self.resource.session
        self.order = Order(self)

    def __getattr__(self, item):
        if item in self.ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

        # Check if the attribute corresponds to a variable alias
        variable = self.resource.variables.by('alias').get(item)

        if variable is None:
            # Variable doesn't exists, must raise an AttributeError
            raise AttributeError('Dataset has no attribute %s' % item)

        # Variable exists!, return the variable entity
        return variable.entity

    def __getitem__(self, item):
        # Check if the attribute corresponds to a variable alias
        variable = self.resource.variables.by('alias').get(item)
        if variable is None:
            # Variable doesn't exists, must raise an ValueError
            raise ValueError('Dataset has no variable %s' % item)

        # Variable exists!, return the variable entity
        return Variable(variable.entity)

    def rename(self, new_name):
        self.resource.edit(name=new_name)

    def stream_rows(self, columns):
        """
        Receives a dict with columns of values to add and streams them
         into the dataset. Client must call .push_rows(n) later.

        Returns the total of rows streamed
        """
        importer = Importer()
        count = len(columns.values()[0])
        for x in range(count):
            importer.stream_rows(self.resource, {a: columns[a][x]
                                                 for a in columns})
        return count

    def push_rows(self, count):
        """
        Batches in the rows that have been currently streamed.
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
        """
        if isinstance(expr, six.string_types):
            expr_obj = parse_expr(expr)
            expr_obj = process_expr(expr_obj, self.resource)  # we need URLs
        elif expr is None:
            expr_obj = {}
        else:
            expr_obj = expr
        return self.resource.session.patch(
            self.resource.fragments.exclusion,
            data=json.dumps(dict(expression=expr_obj))
        )

    def create_categorical(self, categories, rules,
                           name, alias, description='', missing=True):
        """
        creates a categorical variable deriving from other variables
        """
        validate_category_rules(categories, rules)
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
        for rule in rules:
            more_args.append(parse_expr(rule))

        more_args = process_expr(more_args, self.resource)

        expr = dict(function='case', args=args + more_args)

        payload = dict(element='shoji:entity',
                       body=dict(alias=alias,
                                 name=name,
                                 expr=expr,
                                 description=description))

        return self.resource.variables.create(payload)

    def create_multiple_response(self, responses, rules, name, alias,
                                 description=''):
        """
        Creates a Multiple response (array) using a set of rules for each
         of the responses(subvariables).
        """
        raise NotImplementedError()

    def copy_variable(self, variable, name, alias):
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
        shoji_var = self.resource.variables.create(payload).refresh()
        return Variable(shoji_var)

    def combine_categories(self, variable, category_map,
                           name, alias, description=''):
        """
        Create a new variable in the given dataset that is a recode
        of an existing variable
        category_map = {
            1: {
                "name": "Favorable",
                "missing": True,
                "combined_ids": [1,2]
            },
        }
        :param variable: alias of the variable to recode
        :param name: name for the new variable
        :param alias: alias for the new variable
        :param description: description for the new variable
        :param category_map: map to combine categories
        :return: the new created variable
        """
        variable_url = variable_to_url(self.resource, variable)
        categories = validate_category_map(category_map)
        payload = SKELETON.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['expr']['function'] = 'combine_categories'
        payload['body']['expr']['args'] = [
            {
                'variable': variable_url
            },
            {
                'value': categories
            }
        ]
        return Variable(self.resource.variables.create(payload).refresh())

    def combine_responses(self, variable, response_map,
                          name, alias, description=''):
        """
        Creates a new variable in the given dataset that combines existing
        responses into new categorized ones

        response_map = {
            new_subvar_name1:[old_subvar_alias1, old_subvar_alias2],
            new_subvar_name2: [old_subvar_alias3, old_subvar_alias4]
        }
        :return: newly created variable
        """
        variable_url = variable_to_url(self.resource, variable)
        trans_responses = aliases_to_urls(
            self.resource, variable_url, response_map)
        responses = validate_response_map(trans_responses)
        payload = SKELETON.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['expr']['function'] = 'combine_responses'
        payload['body']['expr']['args'] = [
            {
                'variable': variable_url
            },
            {
                'value': responses
            }
        ]
        return self.resource.variables.create(payload)

    def change_editor(self, user):
        """
        Change the current editor of the Crunch dataset.

        Parameters
        ----------
        :param user:
            The email address or the crunch url of the user who should be set
            as the new current editor of the given dataset.

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

            users = self.session.get(api_users).payload['index']

            for url, user in six.iteritems(users):
                if user['email'] == email:
                    user_url = url
                    self.patch({'current_editor': url})
                    break
            assert user_url is not None, 'Unable to resolve user url'

            return user_url

        def _is_url(u):
            return u.startswith('https://') or u.startswith('http://')

        user_url = user if _is_url(user) else _to_url(user)

        self.resource.patch({'current_editor': user_url})

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

        revert = self.resource.savepoints.by('description')\
            .get(description).revert
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
        fork.create_savepoint("initial fork")

        if preserve_owner or '/api/projects/' in self.resource.body.owner:
            try:
                _fork.patch({'owner': self.resource.body.owner})
                _fork.refresh()
            except (pycrunch.ClientError, pycrunch.ServerError):
                # Don't fail if the fork's owner cannot be set to be the same
                # as the parent dataset.
                pass

        return fork

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
            _forks['modification_time'] = pd.to_datetime(
                                            _forks['modification_time'])
            _forks.sort(columns='creation_time', inplace=True)

            return _forks

    def delete_forks(self):
        """
        Deletes all the forks on the dataset. CANNOT BE UNDONE!
        """

        for fork in six.itervalues(self.resource.forks.index):
            fork.entity.delete()

    def download(self, path, filter=None, variables=None, hidden=True):
        """
        Downloads a dataset as CSV to the given path.
        this includes hidden variables and categories
        as id's.
        """
        # the payload should include all hidden variables by default
        payload = {
            "element": "shoji:entity",
            "body": {
                "options": {"use_category_ids": True}
            }
        }
        # add filter to rows if passed
        if filter:
            payload['body']['filter'] = process_expr(
                parse_expr(filter), self.resource)
        # convert variable list to crunch identifiers
        if variables and isinstance(variables, list):
            id_vars = []
            for var in variables:
                id_vars.append(variable_to_url(self.resource, var))
            # Now build the payload with selected variables
            payload['body']['where'] = {
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
            payload['body']['where'] = {
                    'function': 'select',
                    'args': [{
                        'map': {
                            x: {'variable': x}
                            for x in self.resource.variables.index.keys()
                        }
                    }]
                }
        url = export_dataset(self.resource, payload, format='csv')
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
        right_var_url = var_name_to_url(right_ds, right_var)
        left_var_url = var_name_to_url(self, left_var)
        # this dictionary sets the main part of the join
        adapter = {
            'function': 'adapt',
            'args': [
                {'dataset': right_ds.self},
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
                var_url = var_name_to_url(right_ds, var)
                payload['body']['args'][0]['map'][var_url] = {
                    'variable': var_url
                }

        if filter:
            # in the case of a filter, convert it to crunch
            # and attach the filter to the payload
            expr = process_expr(parse_expr(filter), right_ds)
            payload['body']['filter'] = {'expression': expr}

        progress = self.variables.post(payload)
        # poll for progress to finish or return the url to progress
        if wait:
            return wait_progress(r=progress, session=self.session, entity=self)
        return progress.json()['value']


class Variable(object):
    """
    A pycrunch.shoji.Entity wrapper that provides variable-specific methods.
    """

    ENTITY_ATTRIBUTES = {'name', 'alias', 'description', 'discarded', 'format',
                         'type', 'id', 'view', 'notes'}

    def __init__(self, resource):
        self.resource = resource

    def __getattr__(self, item):
        if item in self.ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

    def recode(self, alias=None, map=None, names=None,  # noqa: C901
               default='missing', name=None, description=None):
        """
        Implements SPSS-like recode functionality for Crunch variables.
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
                                           'multiple_response'):
            raise TypeError(
                'Only categorical, categorical_array and multiple_response '
                'variables are supported'
            )

        if name is None:
            name = self.resource.body.name + ' (recoded)'

        if description is None:
            description = self.resource.body.description

        if self.resource.body.type in ('categorical', 'categorical_array'):
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
            payload = SKELETON.copy()
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
            payload = SKELETON.copy()
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

        ds = get_dataset(self.resource.body.dataset_id)
        return ds.variables.create(payload).refresh()

    def edit_categorical(self, categories, rules):
        # validate rules and categories are same size
        validate_category_rules(categories, rules)
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
        ds = get_dataset(self.resource.body.dataset_id)
        more_args = process_expr(more_args, ds)
        # epression value building
        expr = dict(function='case', args=args + more_args)
        payload = dict(
            element='shoji:entity',
            body=dict(expr=expr)
        )
        # patch the variable with the new payload
        return self.resource.patch(payload)

    def edit_derived(self, variable, mapper):
        # get some initial variables
        ds = get_dataset(self.resource.body.dataset_id)
        variable_url = variable_to_url(ds, variable)
        function = self.resource.body.derivation['function']

        # make the proper transformations based on the function
        # array is combine_responses
        if function == 'array':
            trans_responses = aliases_to_urls(ds, variable_url, mapper)
            values = validate_response_map(trans_responses)
            function = 'combine_responses'
        elif function == 'combine_categories':
            values = validate_category_map(mapper)
        else:
            raise AttributeError(
                'Function %s does not support edit' % function)

        # build the proper payload
        payload = {
            'element': 'shoji:entity',
            'body': {
                'expr': {
                    'function': function,
                    'args': [
                        {
                            'variable': variable_url
                        },
                        {
                            'value': values
                        }
                    ]
                }
            }
        }
        return self.resource.patch(payload)

    def hide(self):
        return self.resource.patch(dict(discarded=True))

    def unhide(self):
        return self.resource.patch(dict(discarded=False))

    def edit(self, **kwargs):
        return self.resource.edit(**kwargs)
