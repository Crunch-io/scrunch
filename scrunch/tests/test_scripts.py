# coding: utf-8

import json
from requests import Response
from unittest import TestCase

from pycrunch.shoji import Entity

from scrunch.scripts import DatasetScripts

from .mock_session import MockSession


class TestScripts(TestCase):
    def test_create_script(self):
        session = MockSession()
        scripts_url = "https://example.com/dataset/url/scripts/"

        shoji_resource = Entity(session, **{
            'self': 'https://example.com/dataset/url/',
            'body': {},
            "catalogs": {
                "scripts": scripts_url
            }
        })

        created_script_url = 'https://example.com/script/2/'
        response = Response()
        response.status_code = 201
        response.headers = {
            'Location': created_script_url
        }

        session.add_fixture(scripts_url, {
            "element": "shoji:catalog",
            "self": scripts_url,
            "index": {}
        })
        session.add_post_response(response)
        session.add_fixture(created_script_url, {
            'self': created_script_url,
            'body': {},
        })
        scripts = DatasetScripts(shoji_resource)
        scripts.execute("<script body>")

        post_request = session.requests[-1]
        self.assertEqual(post_request.method, 'POST')
        self.assertEqual(post_request.url, scripts_url)
        self.assertEqual(json.loads(post_request.body), {
            'element': 'shoji:entity',
            'body': {
                'body': "<script body>"
            }
        })

    def test_collapse_scripts(self):
        session = MockSession()
        scripts_url = "https://example.com/dataset/url/scripts/"
        collapse_url = "https://example.com/dataset/url/scripts/collapse/"
        shoji_resource = Entity(session, **{
            'self': 'https://example.com/dataset/url/',
            'body': {},
            "catalogs": {
                "scripts": scripts_url
            }
        })

        response = Response()
        response.status_code = 204

        session.add_fixture(scripts_url, {
            "element": "shoji:catalog",
            "self": scripts_url,
            "index": {},
            "views": {"collapse": collapse_url}
        })
        session.add_fixture(collapse_url, {
            "element": "shoji:view",
            "self": collapse_url,
            "value": {},
        })
        session.add_post_response(response)

        scripts = DatasetScripts(shoji_resource)
        scripts.collapse()

        post_request = session.requests[-1]
        self.assertEqual(post_request.method, 'POST')
        self.assertEqual(post_request.url, collapse_url)
        self.assertEqual(json.loads(post_request.body), {})
