# coding: utf-8

from pycrunch.shoji import Catalog
from scrunch.exceptions import InvalidPathError


class Folder(object):
    def __init__(self, folder_ent, root, parent):
        self.root = root
        self.parent = parent
        self.folder_ent = folder_ent
        self.name = folder_ent.body.name
        self.url = folder_ent.self

    def get(self, path):
        from scrunch.order import Path

        node = self
        for p_name in Path(path).get_parts():
            try:
                node = node.get_child(p_name)
            except KeyError:
                raise InvalidPathError('Subfolder not found %s' % p)
        return node

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

    def make_subfolder(self, folder_name):
        new_ent = self.folder_ent.create(Catalog(self.folder_ent.session, body={
            'name': folder_name
        }))
        self.folder_ent.refresh()
        new_ent.refresh()
        return Folder(new_ent, self.root, self)

    @property
    def children(self):
        self.folder_ent.refresh()  # Always get a fresh copy
        index = self.folder_ent.index
        ds = self.root.dataset
        _children = []
        for item_url in self.folder_ent.graph:
            if item_url in index:
                subfolder = Folder(index[item_url].entity, self.root, self)
                _children.append(subfolder)
            else:
                # Add the variable
                _children.append(ds[item_url])
        return _children

    def move_here(self, *children):
        if not children:
            return
        children = children[0] if isinstance(children[0], list) else children
        index = {c.url: {} for c in children}
        graph = self.folder_ent.graph + [c.url for c in children]
        self.folder_ent.patch({
            'element': 'shoji:catalog',
            'index': index,
            'graph': graph
        })
        self.folder_ent.refresh()

    def rename(self, new_name):
        self.folder_ent.patch({
            'element': 'shoji:catalog',
            'body': {'name': new_name}
        })

    def delete(self):
        self.folder_ent.delete()

    def reorder(self, children):
        graph = self.folder_ent.graph + [c.url for c in children]
        self.folder_ent.patch({
            'element': 'shoji:catalog',
            'graph': graph
        })
        self.folder_ent.refresh()


class DatasetFolders(object):
    def __init__(self, dataset):
        self.enabled = dataset.resource.settings.body.variable_folders
        if self.enabled:
            self.dataset = dataset
            self.root = Folder(dataset.resource.folders, self, None)
            self.hidden = Folder(dataset.resource.folders.hidden, self, None)
            self.trash = Folder(dataset.resource.folders.trash, self, None)

    def get(self, path):
        if self.enabled:
            return self.root.get(path)

    def __getattr__(self, item):
        return super(DatasetFolders, self).__getattribute__(item)
