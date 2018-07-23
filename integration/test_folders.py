# coding: utf-8

"""
Tests that the variable Folders API is properly suported
"""



import os
from unittest import TestCase

from scrunch import connect
from scrunch import get_dataset
from scrunch.datasets import Variable
from scrunch.folders import Folder
from scrunch.exceptions import InvalidPathError
from fixtures import NEWS_DATASET
from pycrunch.shoji import Catalog

HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']

site = connect(username, password, HOST)


def setup_folders(ds):
    sess = ds.session
    ds.settings.edit(variable_folders=True)
    ds.variables.create({
        'element': 'shoji:entity',
        'body': {'name': 'testvar1', 'type': 'numeric'}
    })
    ds.variables.create({
        'element': 'shoji:entity',
        'body': {'name': 'testvar2', 'type': 'numeric'}
    })
    ds.variables.create({
        'element': 'shoji:entity',
        'body': {'name': 'testvar3', 'type': 'numeric'}
    })
    ds.variables.create({
        'element': 'shoji:entity',
        'body': {'name': 'testvar4', 'type': 'numeric'}
    })
    ds.variables.create({
        'element': 'shoji:entity',
        'body': {'name': 'testvar5', 'type': 'numeric'}
    })
    ds.variables.create({
        'element': 'shoji:entity',
        'body': {'name': 'testvar6', 'type': 'numeric'}
    })
    ds.refresh()
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
        setup_folders(ds)
        self.ds = get_dataset(ds.body.id)

    def test_get_folders(self):
        ds = self.ds
        root = ds.folders.get('|')
        sf1 = ds.folders.get('| Subfolder 1')
        sfa = ds.folders.get('| Subfolder 1 | Subfolder A')

        # Equivalent ways of fetching Subfolder A
        sfa2 = root.get('Subfolder 1 | Subfolder A')
        sfa3 = sf1.get('Subfolder A')
        self.assertEqual(sfa.url, sfa2.url)
        self.assertEqual(sfa.url, sfa3.url)

        # Fetching a variable by path
        variable = ds.folders.get('| Subfolder 1 | Subfolder A | Gender')

        self.assertTrue(isinstance(sf1, Folder))
        self.assertTrue(isinstance(sfa, Folder))
        self.assertTrue(isinstance(variable, Variable))

        self.assertEqual(sf1.name, 'Subfolder 1')
        self.assertEqual(sfa.name, 'Subfolder A')
        self.assertEqual(sf1.parent, root)
        self.assertEqual(sfa.parent.name, sf1.name)
        self.assertEqual(sfa.parent.path, sf1.path)
        self.assertEqual(sfa.path, '| Subfolder 1 | Subfolder A')
        self.assertEqual(variable.alias, 'gender')
        self.assertEqual(variable.type, 'categorical')

        bad_path = '| bad folder'
        with self.assertRaises(InvalidPathError) as err:
            ds.folders.get(bad_path)
        self.assertEqual(err.exception.message, "Invalid path: %s" % bad_path)

    def test_create_folder(self):
        ds = self.ds
        root = ds.folders.get('|')
        mit = root.create_folder('Made in test')
        self.assertEqual(mit.path, "| Made in test")
        mit2 = root.get(mit.name)
        self.assertEqual(mit2.url, mit.url)
        nested = mit.create_folder('nested level')
        self.assertEqual(mit.get_child(nested.name).url, nested.url)

    def test_create_folder_with_variables(self):
        ds = self.ds
        root = ds.folders.root
        sf = root.create_folder("with children", alias=['testvar1', 'testvar2'])
        self.assertEqual([Variable, Variable], [type(c) for c in sf.children])
        self.assertEqual(['testvar1', 'testvar2'],
            [c.alias for c in sf.children])

    def test_reorder_folder(self):
        ds = self.ds
        root = ds.folders.get('|')
        folder = root.create_folder('ToReorder')
        sf1 = folder.create_folder('1')
        sf2 = folder.create_folder('2')
        sf3 = folder.create_folder('3')
        var = ds['testvar1']
        folder.move_here([var])
        children = folder.children
        self.assertEqual([c.url for c in children],
            [c.url for c in [sf1, sf2, sf3, var]])
        self.assertEqual(map(type, children),
                         [Folder, Folder, Folder, Variable])

        # Reorder placing sf1 at the end
        folder.reorder([sf2, var, sf3, sf1])
        children = folder.children
        self.assertEqual([c.url for c in children],
            [c.url for c in [sf2, var, sf3, sf1]])

        # Test reorder providing children names only
        folder.reorder(var.alias, sf1.name, sf2.name, sf3.name)
        self.assertEqual([c.url for c in folder.children],
            [c.url for c in [var, sf1, sf2, sf3]])

    def move_to_folder(self):
        ds = self.ds
        sf = ds.folders.root.create_folder("target")
        ds['testvar1'].move_to_folder(sf.path)
        self.assertEqual(['testvar1'], [c.alias for c in sf.children])

    def test_move_between_folders(self):
        root = self.ds.folders.root
        target1 = root.create_folder("t1")
        target2 = root.create_folder("t2")
        self.assertEqual(target1.children, [])
        self.assertEqual(target2.children, [])
        var1 = self.ds['testvar2']
        var2 = self.ds['testvar3']
        target1.move_here([var2, var1])
        self.assertEqual([c.url for c in target1.children],
            [var2.url, var1.url])

        nested = target1.create_folder('nested')
        self.assertEqual([c.url for c in target1.children],
            [var2.url, var1.url, nested.url])

        # Move var2 to t2
        target2.move_here(var2)
        # Now, t1 does not have var2, it is under t2
        self.assertEqual([c.url for c in target1.children],
            [var1.url, nested.url])
        self.assertEqual([c.url for c in target2.children],
            [var2.url])

        # Move nested and var1 to t2
        target2.move_here([nested, var1])

        # Now, they are all in t2, t1 is empty
        self.assertEqual([c.url for c in target2.children],
            [var2.url, nested.url, var1.url])
        self.assertEqual([c.url for c in target1.children], [])

        # Move var1 to nested
        nested.move_here(var1)

        # Now, t2 doesn't have var1, but nested does
        self.assertEqual([c.url for c in target2.children],
            [var2.url, nested.url])
        self.assertEqual([c.url for c in nested.children],
            [var1.url])

        # Move nested to t1
        target1.move_here(nested)

        # Now t2 has only var2 and t1 has nested in it
        self.assertEqual([c.url for c in target2.children],
            [var2.url])
        self.assertEqual([c.url for c in target1.children],
            [nested.url])

    def test_move_here_position(self):
        root = self.ds.folders.root
        sf = root.create_folder("here")
        sf.move_here(self.ds['testvar1'])
        sf.move_here(self.ds['testvar2'], before='testvar1')
        self.assertEqual([c.name for c in sf.children], ['testvar2', 'testvar1'])
        sf.move_here(self.ds['testvar3'], after='testvar2')
        self.assertEqual([c.name for c in sf.children], ['testvar2', 'testvar3', 'testvar1'])
        sf.move_here(self.ds['testvar4'], position=1)
        self.assertEqual([c.name for c in sf.children], ['testvar2', 'testvar4', 'testvar3', 'testvar1'])

    def test_move_by_alias(self):
        root = self.ds.folders.root
        target = root.create_folder("test_move_by_alias")
        target.move_here('testvar2')
        self.assertEqual([c.name for c in target.children], ["testvar2"])

    def test_make_folder_in_position(self):
        root = self.ds.folders.root.create_folder('testhere')
        root.create_folder("p1")
        root.create_folder("p2")
        root.create_folder("p3")

        root.create_folder("A", before="p2")
        self.assertEqual([c.name for c in root.children],
            ["p1", "A", "p2", "p3"])

        root.create_folder("B", after="p2")
        self.assertEqual([c.name for c in root.children],
            ["p1", "A", "p2", "B", "p3"])

        root.create_folder("C", position=3)
        self.assertEqual([c.name for c in root.children],
            ["p1", "A", "p2", "C", "B", "p3"])

    def test_hide_variables(self):
        hidden_folder = self.ds.folders.hidden
        root = self.ds.folders.root
        var1 = self.ds['testvar4']
        var2 = self.ds['testvar5']
        root.move_here(var1, var2)
        hidden_folder.move_here(var2)

        var1_id = var1.resource.body.id
        var2_id = var2.resource.body.id
        self.assertTrue(var2_id in self._ds.folders.hidden.by('id'))
        self.assertTrue(var2_id not in self._ds.folders.by('id'))
        self.assertTrue(var1_id in self._ds.folders.by('id'))
        self.assertTrue(var1_id not in self._ds.folders.hidden.by('id'))

    def test_trash_variables(self):
        trash = self.ds.folders.trash
        root = self.ds.folders.root
        var1 = self.ds['testvar4']
        var2 = self.ds['testvar5']
        root.move_here(var1, var2)
        trash.move_here(var2)

        var1_id = var1.resource.body.id
        var2_id = var2.resource.body.id
        self.assertTrue(var2_id in self._ds.folders.trash.by('id'))
        self.assertTrue(var2_id not in self._ds.folders.by('id'))
        self.assertTrue(var1_id in self._ds.folders.by('id'))
        self.assertTrue(var1_id not in self._ds.folders.trash.by('id'))

    def test_rename(self):
        root = self.ds.folders.root
        sf = root.create_folder('rename me')
        sf.rename("renamed")
        self.assertTrue('renamed' in [c.name for c in root.children])
        self.assertEqual(sf.name, 'renamed')

    def test_delete_folder(self):
        root = self.ds.folders.root
        var = self.ds['testvar6']
        sf = root.create_folder('delete me')
        sf.move_here(var)
        self.assertTrue(var.url in self._ds.variables.index)
        sf.delete()
        # Folder isn't in root anymore
        self.assertFalse(sf.url in [c.url for c in root.children])

    def test_enable_disable(self):
        pycrunch_ds = site.datasets.create({
            'element': 'shoji:entity',
            'body': {
                'name': 'test_folders',
                'table': {
                    'element': 'crunch:table',
                    'metadata': NEWS_DATASET
                },
            }
        }).refresh()
        scrcunch_ds = get_dataset(pycrunch_ds.body.id)
        scrcunch_ds.change_settings(variable_folders=False)
        self.assertFalse(scrcunch_ds.folders.enabled)
        self.assertFalse(hasattr(scrcunch_ds.folders, 'root'))
        scrcunch_ds.change_settings(variable_folders=True)
        self.assertTrue(scrcunch_ds.folders.enabled)
        self.assertTrue(hasattr(scrcunch_ds.folders, 'root'))

    def test_dict_protocol(self):
        root = self.ds.folders.root
        sf1 = root.create_folder("level1")
        sf1a = root.create_folder("level1a")
        sf2 = sf1.create_folder("level2")

        # Access by path []
        self.assertEqual(root[sf2.path].url, sf2.url)

        # Access by name
        self.assertEqual(root[sf1.name].url, sf1.url)
        self.assertEqual(sf1[sf2.name].url, sf2.url)

        # __iter__ method
        self.assertEqual([c.url for c in root], [c.url for c in root.children])

        # more dict methods
        self.assertEqual(set(dict(root).keys()), set(root.keys()))
        self.assertEqual({c.url for c in dict(root).values()},
            {c.url for c in root.children})
        # The right key corresponds to the correct value
        self.assertEqual(dict(root)[sf1.name].url, sf1.url)

    def test_dict_protocol_on_helper(self):
        root = self.ds.folders.root
        sf1 = root.create_folder("level1")
        sf1a = root.create_folder("level1a")
        sf2 = sf1.create_folder("level2")

        top_level = self.ds.folders  # Testing over the helper class
        # Access by path []
        self.assertEqual(top_level[sf2.path].url, sf2.url)

        # Access by name
        self.assertEqual(top_level[sf1.name].url, sf1.url)

        # __iter__ method
        self.assertEqual([c.url for c in top_level],
            [c.url for c in root.children])

        # more dict methods
        self.assertEqual(set(dict(top_level).keys()), set(root.keys()))
        self.assertEqual({c.url for c in dict(top_level).values()},
            {c.url for c in root.children})
        # The right key corresponds to the correct value
        self.assertEqual(dict(top_level)[sf1.name].url, sf1.url)

