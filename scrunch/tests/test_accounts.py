# coding: utf-8

import json
from requests import Response
from scrunch.accounts import Account
from scrunch.datasets import Project
from pycrunch.shoji import Entity, Catalog, View

from .mock_session import MockSession


class TestAccount:
    def make_session(self):
        root_url = "http://host/api/"
        account_url = "http://host/api/account/"
        projects_url = "http://host/api/account/projects/"
        execute_url = "http://host/api/account/run/"

        session = MockSession(site_url=root_url)
        root_resource = Catalog(session, **{
            "element": "shoji:catalog",
            "self": root_url,
            "index": {},
            "catalogs": {},
            "views": {"account": account_url}
        })
        act_resource = Entity(session, **{
            "element": "shoji:entity",
            "self": account_url,
            "body": {
                "name": "test account",
                "id": "00001",
            },
            "catalogs": {
                "projects": projects_url
            },
            "views": {
                "execute": execute_url
            }
        })
        execute_resource = View(session, **{
            "element": "shoji:view",
            "self": execute_url,
            "value": {}
        })
        session.add_fixture(account_url, act_resource)
        session.add_fixture(root_url, root_resource)
        session.add_fixture(execute_url, execute_resource)
        return session

    def test_get_current(self):
        api_root = self.make_session().root
        current_act = Account.current_account(api_root)
        assert current_act.name == "test account"
        assert current_act.id == "00001"
        assert isinstance(current_act, Account)

    def test_execute(self):
        session = self.make_session()

        response = Response()
        response.status_code = 204

        session.add_post_response(response)
        current_act = Account.current_account(session.root)

        current_act.execute("NOOP;")
        post_request = session.requests[-1]
        assert json.loads(post_request.body) == {
            "element": "shoji:view",
            "value": "NOOP;"
        }

    def test_projects(self):
        session = self.make_session()
        projects_url = "http://host/api/account/projects/"
        project_url = "http://host/api/projects/abc/"

        project_resource = Entity(session, **{
            "element": "shoji:entity",
            "self": project_url,
            "body": {
                "name": "my project",
                "id": "abc"
            }
        })

        act_projects_res = Catalog(session, **{
            "element": "shoji:catalog",
            "self": projects_url,
            "index": {
                project_url: {
                    "name": project_resource["body"]["name"],
                    "id": project_resource["body"]["id"],
                }
            }
        })
        session.add_fixture(projects_url, act_projects_res)
        session.add_fixture(project_url, project_resource)
        current_act = Account.current_account(session.root)

        project = current_act.projects.by_name("my project")
        assert project.url == project_url
        assert isinstance(project, Project)

        project = current_act.projects.by_id("abc")
        assert project.url == project_url
        assert isinstance(project, Project)
