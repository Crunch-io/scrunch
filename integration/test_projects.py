# coding: utf-8
import os
from datetime import datetime


from scrunch.datasets import Project
from scrunch.helpers import shoji_entity_wrapper
from scrunch import connect, get_project, get_dataset, get_user
from .fixtures import BaseIntegrationTestCase

UNIQUE_PREFIX = str(datetime.now()).replace(':', '').replace('.', '')
FEATURE_FLAG = 'old_projects_order'


class TestProjects(BaseIntegrationTestCase):
    def setUp(self):
        """
        These tests need to have the `old_projects_order` turned OFF in order
        to enable the new API in Scrunch.
        """
        super(TestProjects, self).setUp()
        self.site.session.feature_flags[FEATURE_FLAG] = False

    def new_project(self, name):
        res = self.site.projects.create(shoji_entity_wrapper({
            "name": name + UNIQUE_PREFIX
        })).refresh()
        return Project(res)

    def test_create_subprojects(self):
        pa = self.new_project('A')
        pb = pa.create_project("B")
        pa.resource.refresh()
        self.assertTrue(pb.url in pa.resource.index)
        _pb = pa.order["| %s" % pb.name]
        self.assertEqual(_pb.url, pb.url)

    def test_move_project(self):
        # Both top level projects
        pa = self.new_project("test_move_project_A")
        pb = self.new_project("test_move_project_B")
        pc = self.new_project("test_move_project_C")
        pa.resource.refresh()
        self.assertFalse(pb.url in pa.resource.index)
        pa.place(pb, "|")
        pa.resource.refresh()
        self.assertTrue(pb.url in pa.resource.index)
        pa.place(pc, "| %s" % pb.name)
        pb.resource.refresh()
        self.assertTrue(pc.url in pb.resource.index)

    def test_rename(self):
        project = self.new_project("test_rename")
        new_name = "renamed" + UNIQUE_PREFIX
        project.rename(new_name)
        _project = get_project(new_name)
        self.assertEqual(_project.url, project.url)

    def test_reorder(self):
        pa = self.new_project('test_reorder')
        p1 = pa.create_project("1")
        p2 = pa.create_project("2")
        pa.resource.refresh()
        self.assertEqual(pa.resource.graph, [p1.url, p2.url])
        pa.reorder(["2", "1"])
        pa.resource.refresh()
        self.assertEqual(pa.resource.graph, [p2.url, p1.url])

    def test_move_dataset(self):
        username_2 = os.environ['SCRUNCH_USER2']
        password_2 = os.environ['SCRUNCH_PASS2']
        fo = get_user(username_2)
        fo_site = connect(fo.email, password_2, self.host)

        project = self.new_project("foo")
        fo_project = fo_site.projects.create(shoji_entity_wrapper({"name": "foo"}))

        # These two datasets are created by the default logged user
        _ds1 = self.site.datasets.create(shoji_entity_wrapper({
            'name': 'test_move_dataset1',
            'project': project.url,
        })).refresh()
        _ds2 = self.site.datasets.create(shoji_entity_wrapper({
            'name': 'test_move_dataset2',
            'project': project.url,
        })).refresh()

        # This dataset is created and owned by the other user
        _ds4 = fo_site.datasets.create(shoji_entity_wrapper({
            'name': 'test_move_dataset4',
            'project': fo_project.self,
        })).refresh()

        ds1 = get_dataset(_ds1.body.id)
        ds2 = get_dataset(_ds2.body.id)
        ds4 = get_dataset(_ds4.body.id, connection=fo_site)

        project.add_user(fo, edit=True)

        # Create a hierarchy A -> B
        pa = self.new_project("test_move_dataset_A")
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
        pa = self.new_project("test_run_script")
        # Successful server execution return a response
        response = pa.execute("NOOP;")
        assert response.ok is True
