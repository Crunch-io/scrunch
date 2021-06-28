# coding: utf-8

import json
from mock import Mock
from unittest import TestCase
from requests import Response

from scrunch.datasets import Team
from scrunch.datasets import User
from .mock_session import MockSession


class TestTeams(TestCase):
    def make_team(self):
        session = MockSession()
        team_url = 'http://example.com/api/teams/ID/'
        team_members_url = 'http://example.com/api/teams/ID/members/'
        team_payload = {
            'self': team_url,
            'element': 'shoji:entity',
            'catalogs': {
                'members': team_members_url
            },
            'body': {
                'name': 'test team'
            }
        }
        team_members_payload = {
            'element': 'shoji:catalog',
            'self': team_members_url,
            'index': {}
        }
        session.add_fixture(team_url, team_payload)
        session.add_fixture(team_members_url, team_members_payload)
        resp = Response()
        resp.status_code = 204
        session.add_patch_response(resp)
        session.add_patch_response(resp)

        team_resource = session.get(team_url).payload
        team = Team(team_resource)
        return session, team

    def make_user(self, user_url):
        user_res = Mock()
        user_res.self = user_url
        user = User(user_res)
        return user

    def test_add_members(self):
        team_members_url = 'http://example.com/api/teams/ID/members/'
        user_url = 'http://example.com/api/users/uid/'
        session, team = self.make_team()
        user = self.make_user(user_url)

        # Add as non editor
        team.members.add(user, edit=False)
        patch_request = session.requests[-1]
        self.assertEqual(patch_request.url, team_members_url)
        self.assertEqual(patch_request.method, 'PATCH')
        self.assertEqual(json.loads(patch_request.body), {
            user_url: {'permissions': {'team_admin': False}}
        })

        # Add as editor
        team.members.add(user, edit=True)
        patch_request = session.requests[-1]
        self.assertEqual(json.loads(patch_request.body), {
            user_url: {'permissions': {'team_admin': True}}
        })

    def test_delete_member(self):
        session, team = self.make_team()
        user_url = 'http://example.com/api/users/uid/'
        team_members_url = 'http://example.com/api/teams/ID/members/'
        user = self.make_user(user_url)
        team.members.remove(user)
        patch_request = session.requests[-1]
        self.assertEqual(patch_request.url, team_members_url)
        self.assertEqual(patch_request.method, 'PATCH')
        self.assertEqual(json.loads(patch_request.body), {
            user_url: None
        })

    def test_list_members(self):
        session, team = self.make_team()
        team_members_url = 'http://example.com/api/teams/ID/members/'

        # without permissions detail
        m = team.members.list(permissions=False)
        get_request = session.requests[-1]
        self.assertEqual(get_request.url, team_members_url)
        self.assertEqual(get_request.method, 'GET')
        assert(isinstance(m, list))

        # with permissions detail
        m = team.members.list(permissions=True)
        get_request = session.requests[-1]
        self.assertEqual(get_request.url, team_members_url)
        self.assertEqual(get_request.method, 'GET')
        assert(isinstance(m, dict))
        assert('edit' in m)
        assert('view' in m)

    def test_delete_team(self):
        mock_resource = Mock()
        team = Team(mock_resource)
        team.delete()
        mock_resource.delete.assert_called_once()
