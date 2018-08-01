# coding: utf-8

import json
from mock import Mock
from requests import Response
from unittest import TestCase

from pycrunch.shoji import Entity, Catalog, Order

from scrunch.datasets import Project, ProjectDatasetsOrder

from .mock_session import MockSession


class TestProjectNesting(TestCase):
    def test_detect_correct_handler(self):
        session = Mock()
        dataset_order = Order(session, **{
            'graph': []
        })
        datasets_catalog = Catalog(session, **{
            'index': {},
            'order': dataset_order
        })
        shoji_resource = Entity(session, **{
            'self': '/project/url/',
            'body': {},
            'index': {},
            'datasets': datasets_catalog
        })
        project = Project(shoji_resource)
        self.assertTrue(isinstance(project.order, ProjectDatasetsOrder))

        shoji_resource = Entity(session, **{
            'self': '/project/url/',
            'body': {},
            'index': {},
            'graph': []  # Look!!
        })
        project = Project(shoji_resource)
        self.assertTrue(isinstance(project.order, Project))

    def test_create_subproject(self):
        session = Mock()
        shoji_resource = Entity(session, **{
            'self': '/project/url/',
            'body': {},
            'index': {},
            'graph': [],
        })
        response = Response()
        response.status_code = 201
        response.headers = {
            'Location': '/project/url/'
        }
        session.post.return_value = response
        project = Project(shoji_resource)

        # Create a new project
        pa = project.order.create_project("Project A")
        self.assertTrue(isinstance(pa, Project))

        # Check that we sent the correct payload to the server
        session.post.assert_called_once()
        func_name, _args, _kwargs = session.post.mock_calls[0]

        # The post happened to the project entity URL
        self.assertEqual(_args[0], project.url)
        # The payload is a valid payload containing the name
        self.assertEqual(json.loads(_args[1]), {
            'element': 'shoji:entity',
            'body': {
                'name': 'Project A'
            }
        })

    def test_follow_path(self):
        session = MockSession()
        a_res_url = 'http://example.com/project/A/'
        b_res_url = 'http://example.com/project/B/'
        c_res_url = 'http://example.com/project/C/'
        d_res_url = 'http://example.com/project/D/'
        a_payload = {
            'element': 'shoji:entity',
            'self': a_res_url,
            'body': {
                'name': 'project A'
            },
            'index': {
                b_res_url: {
                    'id': 'idB',
                    'name': 'project B',
                    'icon': None,
                    'description': '',
                    'type': 'project'
                },
                c_res_url: {
                    'id': 'idC',
                    'name': 'project C',
                    'icon': None,
                    'description': '',
                    'type': 'project'
                }
            },
            'graph': [c_res_url, b_res_url]
        }
        b_payload = {
            'element': 'shoji:entity',
            'self': b_res_url,
            'body': {'name': 'project B'},
            'index': {
                d_res_url: {
                    'id': 'idD',
                    'name': 'project D',
                    'icon': None,
                    'description': '',
                    'type': 'project'
                }
            },
            'graph': [d_res_url]
        }
        c_payload = {
            'element': 'shoji:entity',
            'self': c_res_url,
            'body': {'name': 'project C'},
            'index': {}
        }
        d_payload = {
            'element': 'shoji:entity',
            'self': d_res_url,
            'body': {'name': 'project D'},
            'index': {}
        }
        session.add_fixture(a_res_url, a_payload)
        session.add_fixture(b_res_url, b_payload)
        session.add_fixture(c_res_url, c_payload)
        session.add_fixture(d_res_url, d_payload)
        a_res = Entity(session, **a_payload)
        project_a = Project(a_res)
        project_c = project_a.order['| project C ']
        project_d = project_a.order['| project B | project D']
        self.assertTrue(isinstance(project_d, Project))
        self.assertEqual(project_d.resource.self, d_res_url)
