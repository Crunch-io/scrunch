# coding: utf-8

import pycrunch
from scrunch.helpers import shoji_view_wrapper
from scrunch.scripts import ScriptExecutionError
from scrunch.connections import _default_connection
from scrunch.datasets import Project


class AccountProjects:
    """
    Handles the list of top level projects for the account. Note that this can
    403 if the authenticated user does not have permissions to access the
    projects.
    """

    def __init__(self, resource):
        self.resource = resource

    def by_name(self, name):
        project_res = self.resource.by("name")[name].entity
        return Project(project_res)

    def by_id(self, _id):
        project_res = self.resource.by("id")[_id].entity
        return Project(project_res)


class Account:
    """
    Represents the account for the authenticated user.

    If follows `/api/account/`
    """

    def __init__(self, resource):
        self.resource = resource

    @property
    def name(self):
        return self.resource["body"]["name"]

    @property
    def id(self):
        return self.resource["body"]["id"]

    @classmethod
    def current_account(cls, connection=None):
        site_root = _default_connection(connection)
        act_res = site_root.account
        return cls(act_res)

    def execute(self, script_body):
        """
        Will run a system script on this account.

        System scripts do not have a return value. If they execute correctly
        they'll finish silently. Otherwise an error will raise.
        """
        # The account execution endpoint is a shoji:view
        payload = shoji_view_wrapper(script_body)
        try:
            self.resource.execute.post(payload)
        except pycrunch.ClientError as err:
            resolutions = err.args[2]["resolutions"]
            raise ScriptExecutionError(err, resolutions)

    @property
    def projects(self):
        act_projects = self.resource.projects
        return AccountProjects(act_projects)

