import collections
import json
import re

import pycrunch
import six
from scrunch.exceptions import (InvalidReferenceError, InvalidPathError,
                                OrderUpdateError)
import scrunch.datasets


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
                if 'datasets' not in element or 'variables' in element:
                    # 1. relative variable URL: ../<id>/
                    # 2. compl variable URL: /api/datasets/<id>/variables/<id>/
                    _id = element.split('/')[-2]
                    var = self.order.vars.get(_id)
                    if var:
                        self.elements[var['alias']] = var
                elif 'datasets' in element and 'variables' not in element:
                    # 3. it's a dataset URL
                    _id = element.split('/')[-2]
                    dataset = self.order.datasets.get(_id)
                    if dataset:
                        self.elements[dataset.id] = dataset
            elif isinstance(element, scrunch.datasets.Variable):
                self.elements[element.alias] = element
            elif isinstance(element, scrunch.datasets.Dataset):
                self.elements[element.id] = element
            else:
                subgroup = Group(element, order=self.order, parent=self)
                self.elements[subgroup.name] = subgroup

    def __str__(self):
        def _get_elements(group):
            elements = []
            for key, obj in list(group.elements.items()):
                if isinstance(obj, Group):
                    elements.append({key: _get_elements(obj)})
                elif isinstance(obj, scrunch.datasets.Variable):
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
                'Invalid list of aliases/ids/groups to be inserted'
                ' into the Group.'
            )
        if not all(isinstance(a, six.string_types) for a in alias):
            raise ValueError(
                'Only string references to aliases/ids/group names'
                ' are allowed.'
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

    def create_group(self, name, alias=None, position=-1, before=None,
                     after=None):
        name = self._validate_name_arg(name)
        # when we want to create an empty group
        if not alias:
            self.elements[name] = Group(
                {name: []}, order=self.order, parent=self)
            self.order.update()
            return
        elements = self._validate_alias_arg(alias)
        position = 0 if (before or after) else position

        # create the new Group obj and insert all `elements`
        new_group = Group({name: []}, order=self.order, parent=self)
        new_group.insert(elements)

        # add the new Group to self.elements so that `insert` detects it
        self.elements[name] = new_group
        self.insert(new_group.name, position=position,
                    before=before, after=after)

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

    def move(self, path, position=-1, before=None, after=None):
        position = 0 if (before or after) else position
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
        target_group.insert(
            self.name, position=position, before=before, after=after
        )


class Order(object):

    def __init__(self, resource):
        self.resource = resource
        self._hier = None
        self._datasets = None
        self._graph = None
        self._sync = True
        self._revision = None

    def _load_hier(self):
        """
        subclasses need to overwrite this method in order to get the updated
        order hierarchy
        """
        return self._hier

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
    def graph(self):
        if self._graph is None:
            self._load_graph()
        return self._graph

    @graph.setter
    def graph(self, _):
        raise TypeError('Unsupported assignment operation')

    def _build_graph_structure(self):

        def _get(group):
            _elements = []
            for obj in group.elements.values():
                if isinstance(obj, Group):
                    _elements.append({
                        obj.name: _get(obj)
                    })
                else:
                    if isinstance(obj, scrunch.datasets.Variable):
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


class DatasetVariablesOrder(Order):
    def __init__(self, resource):
        super(DatasetVariablesOrder, self).__init__(resource)
        self._vars = None

    def _load_hier(self):
        self._hier = self.resource.session.get(
            self.resource.variables.orders.hier).payload
        return self._hier

    def _load_vars(self):
        self._vars = self.resource.variables.by('id')
        return self._vars

    @property
    def vars(self):
        if self._vars is None:
            self._load_vars()
        return self._vars

    @vars.setter
    def vars(self, _):
        raise TypeError('Unsupported assignment operation')

    def get(self):
        # Returns the synchronized hierarchical order graph.
        if self._sync:
            ds_state = self.resource.session.get(self.resource.state).payload
            if self._revision is None:
                self._revision = ds_state.body.revision
            elif self._revision != ds_state.body.revision:
                # There's a new dataset revision. Reload the
                # hierarchical order.
                self._revision = ds_state.body.revision
                self._load_hier()
                self._load_graph()
        return self


class ProjectDatasetsOrder(Order):
    def __init__(self, resource):
        super(ProjectDatasetsOrder, self).__init__(resource)

    def _load_hier(self):
        self._hier = self.resource.session.get(
            self.resource.datasets.order.self).payload
        return self._hier

    def _load_datasets(self):
        self._datasets = self.resource.datasets.by('id')
        return self._datasets

    @property
    def datasets(self):
        if self._datasets is None:
            self._load_datasets()
        return self._datasets

    @datasets.setter
    def datasets(self, _):
        raise TypeError('Unsupported assignment operation')

    def get(self):
        # Returns the synchronized hierarchical order graph.
        if self._sync:
            self._load_hier()
            self._load_graph()
        return self
