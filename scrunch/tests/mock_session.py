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
        self.patch_responses = []

    def add_fixture(self, url, fixture):
        self.fixtures[url] = fixture

    def add_post_response(self, response):
        self.post_responses.append(response)

    def add_patch_response(self, response):
        self.patch_responses.append(response)

    def send(self, request, **kwargs):
        self.requests.append(request)
        if request.method == 'POST':
            response = self.post_responses.pop(0)
        elif request.method == "PATCH":
            response = self.patch_responses.pop(0)
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
    token = "xxx"

    def __init__(self, site_url=None):
        self.site_url = site_url or "https://example.com"
        ScrunchSession.__init__(self, token=self.token, site_url=self.site_url)
        self.adapter = FixtureAdapter()
        self.adapters['http://'] = self.adapter
        self.adapters['https://'] = self.adapter

    def add_fixture(self, url, fixture):
        self.adapter.add_fixture(url, fixture)

    def add_post_response(self, response):
        self.adapter.add_post_response(response)

    def add_patch_response(self, response):
        self.adapter.add_patch_response(response)

    def get_fixture(self, url):
        return self.adapter.fixtures[url]

    @property
    def requests(self):
        return self.adapter.requests
