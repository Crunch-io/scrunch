# coding: utf-8

import json
from requests import Response
from requests.adapters import BaseAdapter

from scrunch.session import ScrunchSession

try:
    from StringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO


class FixtureAdapter(BaseAdapter):
    def __init__(self):
        self.fixtures = {}
        self.requests = []
        self.post_responses = []

    def add_fixture(self, url, fixture):
        self.fixtures[url] = fixture

    def add_post_response(self, response):
        self.post_responses.append(response)

    def send(self, request, **kwargs):
        self.requests.append(request)
        if request.method == 'POST':
            response = self.post_responses.pop(0)
        else:
            url = request.url
            if url not in self.fixtures:
                raise NotImplementedError(
                    "URL: %s does not have a fixture" % url)
            response = Response()
            response.status_code = 200
            response.headers['Content-Type'] = 'application/json'
            response.raw = BytesIO(json.dumps(self.fixtures[url]).encode('utf-8'))
        return response


class MockSession(ScrunchSession):
    token = None

    def __init__(self):
        ScrunchSession.__init__(self)
        self.adapter = FixtureAdapter()
        self.adapters['http://'] = self.adapter

    def add_fixture(self, url, fixture):
        self.adapter.add_fixture(url, fixture)

    def add_post_response(self, response):
        self.adapter.add_post_response(response)

    def get_fixture(self, url):
        return self.adapter.fixtures[url]

    @property
    def requests(self):
        return self.adapter.requests
