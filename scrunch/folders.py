# coding: utf-8


class DatasetFolders(object):
    def __init__(self, dataset):
        self.dataset = dataset

    def get(self, path):
        raise NotImplemented

    @property
    def children(self):
        raise NotImplemented

    def move_here(self, children):
        raise NotImplemented

    def rename(self, new_name):
        raise NotImplemented

    def reorder(self, children):
        raise NotImplemented