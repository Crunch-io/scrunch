# coding: utf-8

"""
Tests that the variable Folders API is properly suported
"""



import os
from unittest import TestCase

from scrunch import connect
from scrunch import get_dataset
from fixtures import NEWS_DATASET
from pycrunch.shoji import Catalog

HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']

site = connect(username, password, HOST)


def setup_folders(ds):
    sess = ds.session
    sf1 = Catalog(sess, body={
        'name': 'Subfolder 1'
    })
    sf1 = ds.folders.create(sf1)
    sfa = Catalog(sess, body={
        'name': 'Subfolder A'
    })
    sfa = sf1.create(sfa)
    sf2 = Catalog(sess, body={
            'name': 'Subfolder 2'
        })
    sf2 = ds.folders.create(sf2)
    variables = ds.variables.by('alias')
    sf1.patch({'index': {
        variables['age'].entity_url: {}
    }})
    sfa.patch({'index': {
        variables['gender'].entity_url: {}
    }})
    sf2.patch({'index': {
        variables['socialmedia'].entity_url: {}
    }})


class TestFolders(TestCase):
    def setUp(self):
        self._ds = site.datasets.create({
            'element': 'shoji:entity',
            'body': {
                'name': 'test_folders',
                'table': {
                    'element': 'crunch:table',
                    'metadata': NEWS_DATASET
                },
            }
        }).refresh()
        ds = self._ds
        ds.settings.edit(variable_folders=True)
        ds.refresh()
        setup_folders(ds)
        self.ds = get_dataset(ds.body.id)

    def test_get_folders(self):
        ds = self.ds
        root = ds.folders.get('|')
        sf1 = ds.folders.get('| Subfolder 1')
        sfa = ds.folders.get('| Subfolder 1 | Subfolder A')
        self.assertEqual(sf1.name, 'Subfolder 1')
        self.assertEqual(sfa.name, 'Subfolder A')
        self.assertEqual(sf1.parent, root)
        self.assertEqual(sfa.parent, sf1)

    def _test_hidden_folder(self):
        assert False

    def _test_hide_variable(self):
        assert False

    def _test_make_subfolder(self):
        assert False

    def _test_reorder_folder(self):
        assert False

    def _test_move_between_folders(self):
        assert False
