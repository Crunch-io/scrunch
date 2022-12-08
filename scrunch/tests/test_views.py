# coding: utf-8

import json
import pytest
from mock import Mock, MagicMock
from requests import Response

from pycrunch.shoji import Entity, Catalog, Order

from .mock_session import MockSession
from scrunch.views import DatasetViews
from scrunch.mutable_dataset import MutableDataset


class TestViews:
    def test_disabled_on_views(self):
        session = Mock()
        dataset_shoji_resource = Entity(session, **{
            'self': '/datasets/id/',
            'body': {
                "view_of": None
            },
        })
        view_shoji_resource = Entity(session, **{
            'self': '/datasets/id/',
            'body': {
                "view_of": "some dataset URL"
            },
        })
        for res in (dataset_shoji_resource, view_shoji_resource):
            res.variables = MagicMock()
            res.folders = MagicMock()

        dataset = MutableDataset(dataset_shoji_resource)
        view = MutableDataset(view_shoji_resource)
        assert not dataset.is_view
        assert view.is_view

        with pytest.raises(TypeError) as err:
            view.views  # Accessing this raises on views
        assert str(err.value) == "Cannot access views of views"

        views_handler = dataset.views
        assert isinstance(views_handler, DatasetViews)

    folders_url = "http://host/folders/url"
    public_url = "http://host/folders/public/url"
    project_url = "http://host/project/url/"
    variables_url = "http://host/variables/url/"
    root_url = "http://host/api/"
    datasets_url = "http://host/datasets/url/"

    def _set_folders_fixtures(self, session):
        folders_catalog_res = Catalog(session, **{
            "self": self.folders_url,
            "index": {},
            "catalogs": {
                "public": self.public_url
            },
        })
        public_catalog_res = Catalog(session, **{
            "self": self.public_url,
            "index": {},
            "body": {
                "name": "folder name"
            },
            "catalogs": {},
        })
        project_catalog_res = Catalog(session, **{
            "self": self.project_url,
            "index": {},
            "body": {},
        })
        root_catalog_res = Catalog(session, **{
            "self": self.root_url,
            "index": {},
            "catalogs": {"datasets": self.datasets_url}
        })
        dataset_catalog_res = Catalog(session, **{
            "self": self.datasets_url,
            "index": {},
        })
        session.site_url = self.root_url
        session.add_fixture(self.folders_url, folders_catalog_res)
        session.add_fixture(self.public_url, public_catalog_res)
        session.add_fixture(self.project_url, project_catalog_res)
        session.add_fixture(self.root_url, root_catalog_res)
        session.add_fixture(self.datasets_url, dataset_catalog_res)

    def test_create_view_no_cols(self):
        session = MockSession()
        views_url = "http://host/views/url/"
        dataset_url = "http://host/datasets/id/"
        variables_url = "http://host/variables/url/"
        new_view_url = "http://host/new/view/url/"
        project_url = "http://host/project/id"
        ds_res = Entity(session, **{
            'self': dataset_url,
            'body': {
                "view_of": None,
                "owner": project_url
            },
            "catalogs": {
                "views": views_url,
                "variables": variables_url,
                "parent": self.project_url
            }
        })
        variables_catalog_res = Catalog(session, **{
            "self": self.variables_url,
            "index": {}
        })
        new_view_res = Entity(session, **{
            "self": new_view_url,
            "body": {
                "view_of": dataset_url
            },
            "catalogs": {
                "variables": variables_url,  # Same as dataset, unimportant
                "folders": self.folders_url
            }
        })
        self._set_folders_fixtures(session)
        session.add_fixture(self.variables_url, variables_catalog_res)

        create_response = Response()
        create_response.status_code = 201
        create_response.headers = {
            "Location": new_view_url
        }
        session.add_post_response(create_response)
        session.add_fixture(new_view_url, new_view_res)

        views = DatasetViews(ds_res)
        new_view = views.create("My view", None)
        assert isinstance(new_view, MutableDataset)
        assert new_view.is_view
        assert new_view.resource.body["view_of"] == dataset_url
        create_body = json.loads(session.requests[2].body)
        assert create_body == {
            "element": "shoji:entity",
            "body": {
                "name": "My view",
                "view_of": dataset_url,
                "owner": project_url,
            },
        }

    def test_create_view_with_cols(self):
        session = MockSession()
        views_url = "http://host/views/url/"
        dataset_url = "http://host/datasets/id/"
        new_view_url = "http://host/new/view/url/"
        project_url = "http://host/project/id"
        ds_res = Entity(session, **{
            'self': dataset_url,
            'body': {
                "view_of": None,
                "owner": project_url,
            },
            "catalogs": {
                "views": views_url,
                "variables": self.variables_url,
                "parent": self.project_url
            }
        })
        variables_catalog_res = Catalog(session, **{
            "self": self.variables_url,
            "index": {
                "idA": {"alias": "A"},
                "idB": {"alias": "B"},
            }
        })
        new_view_res = Entity(session, **{
            "self": new_view_url,
            "body": {
                "view_of": dataset_url
            },
            "catalogs": {
                "variables": self.variables_url,  # Same as dataset, unimportant
                "folders": self.folders_url
            }
        })
        self._set_folders_fixtures(session)
        session.add_fixture(self.variables_url, variables_catalog_res)

        create_response = Response()
        create_response.status_code = 201
        create_response.headers = {
            "Location": new_view_url
        }
        session.add_post_response(create_response)
        session.add_fixture(new_view_url, new_view_res)

        views = DatasetViews(ds_res)
        views.create("My view", ["A", "B"])
        create_body = json.loads(session.requests[3].body)
        assert create_body == {
            "element": "shoji:entity",
            "body": {
                "view_cols": ["idA", "idB"],
                "name": "My view",
                "view_of": dataset_url,
                "owner": project_url
            },
        }

    def test_all_names(self):
        session = MockSession()
        views_url = "http://host/views/url/"
        dataset_url = "http://host/datasets/id/"
        ds_res = Entity(session, **{
            'self': dataset_url,
            'body': {
                "view_of": None
            },
            "catalogs": {
                "views": views_url,
                "variables": self.variables_url,
                "parent": self.project_url
            }
        })
        views_catalog = Catalog(session, **{
            "self": views_url,
            "index": {
                "v1_url": {"name": "view 1"},
                "v2_url": {"name": "view 2"},
            }
        })
        session.add_fixture(views_url, views_catalog)
        views = DatasetViews(ds_res)
        view_names = views.all_names()
        assert view_names == {"view 1", "view 2"}

    def test_get_by_name(self):
        session = MockSession()
        views_url = "http://host/views/url/"
        dataset_url = "http://host/datasets/id/"
        variables_url = "http://host/variables/url/"
        view_1_url = "http://host/view/1/"
        ds_res = Entity(session, **{
            'self': dataset_url,
            'body': {
                "view_of": None
            },
            "catalogs": {
                "views": views_url,
                "variables": variables_url,
                "parent": self.project_url
            }
        })
        views_catalog = Catalog(session, **{
            "self": views_url,
            "index": {
                view_1_url: {"name": "view 1"},
                "v2_url": {"name": "view 2"},
            }
        })

        view_1_res = Entity(session, **{
            "self": view_1_url,
            "body": {
                "view_of": dataset_url
            },
            "catalogs": {
                "variables": variables_url,  # Same as dataset, unimportant
                "folders": self.folders_url
            }
        })
        self._set_folders_fixtures(session)
        session.add_fixture(self.variables_url, {"index": {}})
        session.add_fixture(views_url, views_catalog)
        session.add_fixture(view_1_url, view_1_res)
        views = DatasetViews(ds_res)
        view_1 = views.get_by_name("view 1")
        assert view_1.url == view_1_url
