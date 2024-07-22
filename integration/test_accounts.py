# coding: utf-8

import uuid
import pytest

from pycrunch.shoji import as_entity

from fixtures import BaseIntegrationTestCase
from scrunch.accounts import Account


class TestAccount(BaseIntegrationTestCase):

    def test_current_account(self):
        act = Account.current_account(self.site)
        assert act.resource.self == self.site.account.self

    def test_execute(self):
        raise pytest.skip("Wait until account execute is implemented in backend")

    def test_account_projects(self):
        # Create a top level project
        project_name = uuid.uuid4().hex
        api_project = self.site.projects.create(as_entity({"name": project_name}))
        api_project.refresh()
        act = Account.current_account(self.site)

        project = act.projects.by_name(project_name)
        assert project.url == api_project.self
        assert project.name == project_name

        project = act.projects.by_id(api_project.body["id"])
        assert project.url == api_project.self

        project.delete()
