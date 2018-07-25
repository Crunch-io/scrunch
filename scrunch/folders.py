# coding: utf-8

from pycrunch.shoji import Catalog
from scrunch.exceptions import InvalidPathError


class Folder(object):
    def __init__(self, folder_ent, root, parent):
        self.root = root
        self.parent = parent
        self.folder_ent = folder_ent
        self.name = folder_ent.body.name
        self.alias = self.name  # For compatibility with variables .alias
        self.url = folder_ent.self

    def __repr__(self):
        return '<Folder: %s>' % self.name

    def get(self, path):
        self.folder_ent.refresh()  # Always up to date
        from scrunch.order import Path

        node = self
        for p_name in Path(path).get_parts():
            try:
                node = node.get_child(p_name)
            except KeyError:
                raise InvalidPathError('Subfolder not found %s' % p)
        return node

    def __getitem__(self, path):
        return self.get(path)

    def __iter__(self):
        return self.itervalues()

    def iterkeys(self):
        for child in self.children:
            yield child.alias

    def itervalues(self):
        for child in self.children:
            yield child

    def iteritems(self):
        for child in self.children:
            yield (child.alias, child)

    def values(self):
        return list(self.itervalues())

    def keys(self):
        return list(self.iterkeys())

    def items(self):
        return list(self.iteritems())

    def get_child(self, name):
        by_name = self.folder_ent.by('name')
        by_alias = self.folder_ent.by('alias')

        # If found by alias, then it's a variable, return the variable
        if name in by_alias:
            return self.root.dataset[name]
        elif name in by_name:
            # Found by name, if it's not a folder, return the variable
            tup = by_name[name]
            if tup.type != 'folder':
                return self.root.dataset[name]
            return Folder(tup.entity, self.root, self)

        # Not a folder nor a variable
        path = self.path_pieces() + [name]
        raise InvalidPathError('Invalid path: | %s' % ' | '.join(path))

    def path_pieces(self):
        if self.parent:
            return self.parent.path_pieces() + [self.name]
        return []

    @property
    def path(self):
        return '| ' + ' | '.join(self.path_pieces())

    def create_folder(self, folder_name, position=None, after=None, before=None, alias=None):
        new_ent = self.folder_ent.create(Catalog(self.folder_ent.session, body={
            'name': folder_name
        }))
        new_ent.refresh()
        subfolder = Folder(new_ent, self.root, self)
        if position is not None or after is not None or before is not None:
            children = self._position_items([subfolder], position, before, after)
            self.reorder(children)

        if alias:
            subfolder.move_here(alias)

        self.folder_ent.refresh()
        return subfolder

    def _position_items(self, new_items, position, before, after):
        if before is not None or after is not None:
            # Before and After are strings
            target = before or after
            position = [x for x, c in enumerate(self.children) if c.alias == target]
            if not position:
                raise InvalidPathError("No child with name %s found" % target)
            position = position[0]
            if before is not None:
                position = position if position > 0 else 0
            else:
                max_pos = len(self.folder_ent.graph)
                position = (position + 1) if position < max_pos else max_pos

        if position is not None:
            new_urls = {c.url for c in new_items}
            children = [c for c in self.children if c.url not in new_urls]
            for item in reversed(new_items):
                children.insert(position, item)
            return children
        return self.children  # Nothing happened

    @property
    def children(self):
        self.folder_ent.refresh()  # Always get a fresh copy
        index = self.folder_ent.index
        ds = self.root.dataset
        _children = []
        for item_url in self.folder_ent.graph:
            if item_url not in index:
                continue
            item_tup = index[item_url]
            if item_tup['type'] == 'folder':
                subfolder = Folder(item_tup.entity, self.root, self)
                _children.append(subfolder)
            else:
                # Add the variable
                _children.append(ds[item_tup['alias']])
        return _children

    def move_here(self, *children, **kwargs):
        if not children:
            return
        children = children[0] if isinstance(children[0], list) else children
        children = [
            self.root.dataset[c] if isinstance(c, basestring) else c
            for c in children
        ]
        index = {c.url: {} for c in children}
        position, before, after = [kwargs.get('position'),
                                   kwargs.get('before'), kwargs.get('after')]
        if position is not None or after is not None or before is not None:
            children = self._position_items(children, position, before, after)
        graph = self.folder_ent.graph + [c.url for c in children]
        self.folder_ent.patch({
            'element': 'shoji:catalog',
            'index': index,
            'graph': graph
        })
        self.folder_ent.refresh()

    def append(self, *children):
        """Alias of .move_here, adds items at the end of the folder"""
        self.move_here(*children)

    def insert(self, *children, **kwargs):
        """Alias of move_here with a specific position"""
        self.move_here(*children, position=kwargs.get('position', 0))

    def move(self, path, position=None, after=None, before=None):
        """
        The inverse of move here, but it allows to move only folder by folder.
        This API is much more chatty than using .move_here() that allows to
        move multiple items in one call. Implemented for orders backwards compat
        """
        target = self.root.get(path)
        target.move_here(self, position=position, after=after, before=before)

    def rename(self, new_name):
        self.folder_ent.patch({
            'element': 'shoji:catalog',
            'body': {'name': new_name}
        })
        self.name = new_name

    def delete(self):
        self.folder_ent.delete()

    def reorder(self, *items):
        items = items[0] if isinstance(items[0], list) else items
        name2tup = self.folder_ent.by('name')
        graph = [
            name2tup[c].entity_url if isinstance(c, basestring) else c.url
            for c in items
        ]
        self.folder_ent.patch({
            'element': 'shoji:catalog',
            'graph': graph
        })
        self.folder_ent.refresh()


class DatasetFolders(object):
    def __init__(self, dataset):
        self.enabled = dataset.resource.settings.body.variable_folders
        self.dataset = dataset
        if self.enabled:
            self.dataset = dataset
            self.root = Folder(dataset.resource.folders, self, None)
            self.hidden = Folder(dataset.resource.folders.hidden, self, None)
            self.trash = Folder(dataset.resource.folders.trash, self, None)

    def get(self, path):
        if self.enabled:
            return self.root.get(path)

    def __getitem__(self, path):
        return self.root.get(path)

    def __iter__(self):
        return self.root.itervalues()

    def iterkeys(self):
        for child in self.root.children:
            yield child.alias

    def itervalues(self):
        for child in self.root.children:
            yield child

    def iteritems(self):
        for child in self.root.children:
            yield (child.alias, child)

    def values(self):
        return list(self.itervalues())

    def keys(self):
        return list(self.iterkeys())

    def items(self):
        return list(self.iteritems())
