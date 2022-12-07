# coding: utf-8

import os
import uuid
import pytest
from scrunch import connect
from scrunch.accounts import Account

as_entity = lambda b: {
    "element": "shoji:entity",
    "body": b
}


HOST = os.environ["SCRUNCH_HOST"]
username = os.environ["SCRUNCH_USER"]
password = os.environ["SCRUNCH_PASS"]

site = connect(username, password, HOST)
assert site is not None, "Unable to connect to %s" % HOST


class TestAccount:

    def test_current_account(self):
        act = Account.current_account(site)
        assert act.resource.self == site.account.self

    def test_execute(self):
        raise pytest.skip("Wait until account execute is implemented in backend")

    def test_account_projects(self):
        # Create a top level project
        project_name = uuid.uuid4().hex
        api_project = site.projects.create(as_entity({"name": project_name}))
        api_project.refresh()
        act = Account.current_account(site)

        project = act.projects.by_name(project_name)
        assert project.url == api_project.self
        assert project.name == project_name

        project = act.projects.by_id(api_project.body["id"])
        assert project.url == api_project.self

        project.delete()
