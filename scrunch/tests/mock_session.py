# coding: utf-8

import json
from requests import Response
from requests.adapters import BaseAdapter

from scrunch.session import ScrunchSession

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class FixtureAdapter(BaseAdapter):
    def __init__(self):
        self.fixtures = {}
        self.requests = []

    def add_fixture(self, url, fixture):
        self.fixtures[url] = fixture

    def send(self, request, **kwargs):
        self.requests.append(request)
        url = request.url
        if url not in self.fixtures:
            raise NotImplementedError("URL: %s does not have a fixture" % url)
        response = Response()
        response.status_code = 200
        response.headers['Content-Type'] = 'application/json'
        response.raw = StringIO(json.dumps(self.fixtures[url]))
        return response


class MockSession(ScrunchSession):
    token = None

    def __init__(self):
        ScrunchSession.__init__(self)
        self.adapter = FixtureAdapter()
        self.adapters['http://'] = self.adapter

    def add_fixture(self, url, fixture):
        self.adapter.add_fixture(url, fixture)

    def get_fixture(self, url):
        return self.adapter.fixtures[url]

    @property
    def requests(self):
        return self.adapter.requests
