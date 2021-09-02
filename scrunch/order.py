import collections
import json
import re

import six

import pycrunch
import scrunch.datasets
from scrunch.exceptions import (InvalidPathError, InvalidReferenceError,
                                OrderUpdateError)

NAME_REGEX = re.compile(r'^\|$|^\|?([\w\s,&\(\)\-\/\\]+\|?)+$', re.UNICODE)


class Path(object):
    def __init__(self, path):
        if not isinstance(path, six.string_types):
            raise TypeError('The path must be a string object')

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
                # NOTE: instantiating Variable/Dataset here seems overkill to
                # me. While its as simple as `Dataset(dataset.entity)` for the
                # `dataset` tuple below, for the Variable we would first need
                # to instantiate it's `Dataset`, going through all this order
                # machinery again...
                if 'datasets' not in element or 'variables' in element:
                    # 1. relative variable URL: ../<id>/
                    # 2. compl variable URL: /api/datasets/<id>/variables/<id>/
                    var = self.order.vars.get(_id)
                    if var:
                        self.elements[var.alias] = var
                elif 'datasets' in element and 'variables' not in element:
                    # 3. it's a dataset URL
                    dataset = self.order.datasets.get(_id)
                    if dataset:
                        self.elements[dataset.id] = dataset
                    else:
                        refresh_ds = self.order.catalog.refresh()['index']
                        for key, obj in refresh_ds.items():
                            if _id in key:
                                self.elements[obj.id] = obj
                                break
            elif isinstance(element, dict):
                subgroup = Group(element, order=self.order, parent=self)
                self.elements[subgroup.name] = subgroup
            # TODO unreached code
            elif isinstance(element, scrunch.datasets.Variable):
                self.elements[element.alias] = element
            elif isinstance(element, scrunch.datasets.BaseDataset):
                self.elements[element.id] = element
            else:
                raise TypeError('Invalid OrderObject %s' % element)

    def __str__(self):
        def _get_elements(group):
            elements = []
            for key, obj in list(group.elements.items()):
                if isinstance(obj, Group):
                    elements.append({key: _get_elements(obj)})
                # TODO unreached code
                elif isinstance(obj, (scrunch.datasets.Variable,
                                      scrunch.datasets.BaseDataset)):
                    elements.append(obj.name)
                else:
                    elements.append(obj.name)
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

    def __delitem__(self, item):
        if len(self.elements[item].keys()) == 0:
            del self.elements[item]
            self.order.update()
        else:
            raise ValueError(
                "Cannot delete Groups containing items. Group must be empty first"
            )

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
        if '|' in name:
            raise ValueError(
                'The pipe (|) character is not allowed.'
            )
        if name in self.elements:
            raise ValueError(
                'A variable/sub-group named \'%s\' already exists.' % name
            )

        if six.PY2:
            regex_match = re.match(NAME_REGEX, name.decode('utf-8'))
        else:
            regex_match = re.match(NAME_REGEX, name)

        if not regex_match:
            raise ValueError("Invalid character in name: %s" % name)

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
                current_group = self.order.group.find(element_name)
                if current_group:
                    # A variable.
                    elements_to_move[element_name] = \
                        (current_group, '__migrate_element__')
                else:
                    # Not a variable. A group, maybe?
                    group_to_move = self.order.group.find_group(element_name)
                    if group_to_move:
                        elements_to_move[element_name] = \
                            (group_to_move, '__migrate_group__')
                    else:
                        raise ValueError(
                            'Invalid alias/id/group name \'%s\'' % element_name
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
                    elif operation == '__migrate_element__':
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
    """
    How do you expect to use this class?

    examples:
        $ Project.order
        ...
        $ Project.order['|'].create_group(
            'FirstGroup',entities=[ds1.id, ds2.id], before..., after...)
        $ Project.order['|'].create_group('2ndGroup')
    """

    def __init__(self, catalog, order):
        self.catalog = catalog
        self.order = order
        self._load(refresh=False)

    def _load(self, refresh=True):
        """
        Do not call this method manually, this is intended for
        methods only
        """
        if refresh:
            self.catalog.refresh()
            self.order.refresh()
        self.group = Group({'__root__': self.order.graph}, order=self)

    def place(self, entity, path, position=-1, before=None, after=None):
        """
        place an entity into a specific place in the order hierarchy
        """
        position = 0 if (before or after) else position
        path = Path(path)
        if not path.is_absolute:
            raise InvalidPathError(
                'Invalid path %s: only absolute paths are allowed.' % path
            )
        target_group = self.group[str(path)]
        if isinstance(entity, scrunch.datasets.Variable):
            element = entity.alias
        elif isinstance(entity, scrunch.datasets.BaseDataset):
            element = entity.id
        else:
            raise TypeError('entity must be a `Variable` or `Dataset`')

        target_group.insert(
            element, position=position,
            before=before, after=after)

    def _prepare_shoji_graph(self):
        """
        returns shoji:graph ready for the server
        """

        def _get(group):
            _elements = []
            for obj in group.elements.values():
                if isinstance(obj, Group):
                    _elements.append({
                        obj.name: _get(obj)
                    })
                else:
                    url = obj.entity.self
                    _elements.append(url)
            return _elements

        return _get(self.group)

    def update(self):
        updated_order = {
            'element': 'shoji:order',
            'graph': self._prepare_shoji_graph()
        }
        try:
            # NOTE: Order has no Attribute edit
            self.order.put(updated_order)
        except pycrunch.ClientError as e:
            # Our update to the Hierarchical Order failed. Better reload.
            self._load(refresh=True)
            raise OrderUpdateError(str(e))

    # Proxy methods for the __root__ Group

    def __str__(self):
        return str(self.group)

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        return self.group.itervalues()

    def itervalues(self):
        return self.group.itervalues()

    def iterkeys(self):
        return self.group.iterkeys()

    def keys(self):
        return self.group.keys()

    def values(self):
        return self.group.values()

    def items(self):
        return self.group.items()

    def __getitem__(self, item):
        return self.group[item]


class DatasetVariablesOrder(Order):

    def _load(self, refresh=True):
        self.vars = self.catalog.by('id')
        super(DatasetVariablesOrder, self)._load(refresh=refresh)


class ProjectDatasetsOrder(Order):

    def _load(self, refresh=False):
        self.datasets = self.catalog.by('id')
        super(ProjectDatasetsOrder, self)._load(refresh=refresh)
