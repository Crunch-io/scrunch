# coding: utf-8

import os
from datetime import datetime
from unittest import TestCase

from scrunch import connect, get_project
from scrunch.datasets import Project
from scrunch.helpers import shoji_entity_wrapper


HOST = os.environ['SCRUNCH_HOST']
username = os.environ['SCRUNCH_USER']
password = os.environ['SCRUNCH_PASS']

site = connect(username, password, HOST)
UNIQUE_PREFIX = unicode(datetime.now()).replace(':', '').replace('.', '')


def new_project(name):
    res = site.projects.create(shoji_entity_wrapper({
        "name": name + UNIQUE_PREFIX
    }))
    return Project(res)


class TestProjects(TestCase):
    def test_create_subprojects(self):
        raise self.skipTest("Wait until Crunch.io API is available")
        pa = new_project('A')
        pb = pa.create_project("B")
        pa.resource.refresh()
        self.assertTrue(pb.url in pa.resource.index)
        _pb = pa.order["| %s" % pb.name]
        self.assertEqual(_pb.url, pb.url)

    def test_move_project(self):
        raise self.skipTest("Wait until Crunch.io API is available")
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
        raise self.skipTest("Wait until Crunch.io API is available")
        project = new_project("test_rename")
        new_name = "renamed" + UNIQUE_PREFIX
        project.rename(new_name)
        _project = get_project(new_name)
        self.assertEqual(_project.url, project.url)

    def test_reorder(self):
        raise self.skipTest("Wait until Crunch.io API is available")
        pa = new_project('test_reorder')
        p1 = pa.create_project("1")
        p2 = pa.create_project("2")
        pa.resource.refresh()
        self.assertEqual(pa.resource.graph, [p1.url, p2.url])
        pa.reorder(["2", "1"])
        pa.resource.refresh()
        self.assertEqual(pa.resource.graph, [p2.url, p1.url])
