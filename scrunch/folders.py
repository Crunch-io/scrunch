# coding: utf-8

from scrunch.exceptions import InvalidPathError


def get_path(node, path):
    from scrunch.order import Path

    for p_name in Path(path).get_parts():
        try:
            node = node.get_child(p_name)
        except KeyError:
            raise InvalidPathError('Subfolder not found %s' % p)
    return node


class Folder(object):
    def __init__(self, folder_ent, root, parent):
        self.root = root
        self.parent = parent
        self.folder_ent = folder_ent
        self.name = folder_ent.body.name

    def get(self, path):
        return get_path(self, path)

    def get_child(self, name):
        by_name = self.folder_ent.by('name')
        by_alias = self.folder_ent.by('alias')
        print "Names", by_name.keys()
        print "alioase", by_alias.keys()

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


class DatasetFolders(object):
    def __init__(self, dataset):
        self.dataset = dataset
        self.root = Folder(dataset.resource.folders, self, None)

    def get(self, path):
        return get_path(self.root, path)

    @property
    def children(self):
        raise NotImplemented

    def move_here(self, children):
        raise NotImplemented

    def rename(self, new_name):
        raise NotImplemented

    def reorder(self, children):
        raise NotImplemented
