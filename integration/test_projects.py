# coding: utf-8

import os
from unittest import TestCase

from scrunch import connect, get_project
from scrunch.datasets import Project
from scrunch.helpers import shoji_entity_wrapper


HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']

site = connect(username, password, HOST)


def new_project(name):
    res = site.projects.create(shoji_entity_wrapper({
        "name": name
    }))
    return Project(res)


class TestProjects(TestCase):
    def test_create_subprojects(self):
        pa = new_project('Ax')
        pb = pa.create_project("B")
        pa.resource.refresh()
        self.assertTrue(pb.url in pa.resource.index)

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
        project.rename("renamed")
        _project = get_project("renamed")
        self.assertEqual(_project.url, project.url)

