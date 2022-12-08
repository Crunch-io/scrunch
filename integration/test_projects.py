# coding: utf-8

import os
from datetime import datetime
from unittest import TestCase

from scrunch.datasets import Project
from scrunch.helpers import shoji_entity_wrapper
from scrunch import connect, get_project, get_dataset, get_user


HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']
username2 = os.environ['SCRUNCH_USER2']
password2 = os.environ['SCRUNCH_PASS2']

site = connect(username, password, HOST)
UNIQUE_PREFIX = str(datetime.now()).replace(':', '').replace('.', '')
FEATURE_FLAG = 'old_projects_order'


def new_project(name):
    res = site.projects.create(shoji_entity_wrapper({
        "name": name + UNIQUE_PREFIX
    })).refresh()
    return Project(res)


class TestProjects(TestCase):
    def setUp(self):
        """
        These tests need to have the `old_projects_order` turned OFF in order
        to enable the new API in Scrunch.
        """
        site.session.feature_flags[FEATURE_FLAG] = False

    def test_create_subprojects(self):
        pa = new_project('A')
        pb = pa.create_project("B")
        pa.resource.refresh()
        self.assertTrue(pb.url in pa.resource.index)
        _pb = pa.order["| %s" % pb.name]
        self.assertEqual(_pb.url, pb.url)

    def test_move_project(self):
        # Both top level projects
        pa = new_project("test_move_project_A")
        pb = new_project("test_move_project_B")
        pc = new_project("test_move_project_C")
        pa.resource.refresh()
        self.assertFalse(pb.url in pa.resource.index)
        pa.place(pb, "|")
        pa.resource.refresh()
        self.assertTrue(pb.url in pa.resource.index)
        pa.place(pc, "| %s" % pb.name)
        pb.resource.refresh()
        self.assertTrue(pc.url in pb.resource.index)

    def test_rename(self):
        project = new_project("test_rename")
        new_name = "renamed" + UNIQUE_PREFIX
        project.rename(new_name)
        _project = get_project(new_name)
        self.assertEqual(_project.url, project.url)

    def test_reorder(self):
        pa = new_project('test_reorder')
        p1 = pa.create_project("1")
        p2 = pa.create_project("2")
        pa.resource.refresh()
        self.assertEqual(pa.resource.graph, [p1.url, p2.url])
        pa.reorder(["2", "1"])
        pa.resource.refresh()
        self.assertEqual(pa.resource.graph, [p2.url, p1.url])

    def test_move_dataset(self):
        fo = get_user(username2)
        fo_site = connect(fo.email, password2, HOST)

        # These two datasets are created by the default logged user
        _ds1 = site.datasets.create(shoji_entity_wrapper({
            'name': 'test_move_dataset1'
        })).refresh()
        _ds2 = site.datasets.create(shoji_entity_wrapper({
            'name': 'test_move_dataset2'
        })).refresh()

        # This dataset is created and owned by the other user
        _ds4 = fo_site.datasets.create(shoji_entity_wrapper({
            'name': 'test_move_dataset4',
            'owner': fo.url
        })).refresh()

        ds1 = get_dataset(_ds1.body.id)
        ds2 = get_dataset(_ds2.body.id)
        ds4 = get_dataset(_ds4.body.id, connection=fo_site)

        ds2.add_user(fo, edit=True)

        # Create a hierarchy A -> B
        pa = new_project("test_move_dataset_A")
        pa.move_here([ds1])  # Put ds1 in A

        pb = pa.create_project("test_move_dataset_B")
        pa.add_user(fo, edit=True)

        # Move ds4 to B as the other user
        fo_pa = get_project(pa.name, fo_site)
        fo_pa.place(ds4, path="| %s" % pb.name)
        pb.resource.refresh()
        # Only ds4 here
        assert sorted(pb.resource.index.keys()) == [_ds4.self]

        fo_ds1 = get_dataset(_ds1.body.id, connection=fo_site)
        fo_pa.place(fo_ds1, path="| %s" % pb.name)

        pb.resource.refresh()
        assert sorted(pb.resource.index.keys()) == sorted([_ds1.self, _ds4.self])
        pa.place(ds2, path="| %s" % pb.name)

        pb.resource.refresh()
        assert sorted(pb.resource.index.keys()) == sorted([_ds1.self, _ds2.self, _ds4.self])
        self.assertEqual(ds2.resource.project.self, pb.url)

    def test_execute_script(self):
        pa = new_project("test_run_script")
        # Successful server execution does not raise. Method returns None on OK
        assert pa.execute("NOOP;") is None
