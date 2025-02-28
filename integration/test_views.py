# coding: utf-8

from pycrunch.shoji import as_entity

from scrunch.mutable_dataset import get_mutable_dataset
from .fixtures import BaseIntegrationTestCase


class TestViews(BaseIntegrationTestCase):
    FIXTURE_VARIABLES = [
        ("var1", "numeric"),
        ("var2", "text"),
        ("var3", "numeric"),
    ]

    def _create_ds(self):
        project = self.site.projects.create(as_entity({"name": "foo"}))
        ds = self.site.datasets.create(
            as_entity({"name": "test_script", "project": project.self})
        ).refresh()

        for alias, v_type in self.FIXTURE_VARIABLES:
            var_body = as_entity({
                "name": alias,
                "alias": alias,
                "type": v_type,
            })
            ds.variables.create(var_body)

        return ds

    def test_create_view(self):
        api_ds = self._create_ds()
        scrunch_dataset = get_mutable_dataset(api_ds.body.id, self.site, editor=True)
        new_view = scrunch_dataset.views.create("My first view", None)
        # Assert it is a view
        assert new_view.resource.body["view_of"] == api_ds.self

        # Assert the variables are here
        for alias, v_type in self.FIXTURE_VARIABLES:
            assert new_view[alias].type == v_type

    def test_create_view_non_personal_ds(self):
        api_ds = self._create_ds()

        # Put the dataset in a public project
        project = self.site.projects.create(as_entity({"name": "public project"}))
        project.patch({"index": {api_ds.self: {}}})

        scrunch_dataset = get_mutable_dataset(api_ds.body.id, self.site, editor=True)
        new_view = scrunch_dataset.views.create("My view", None)

        # Assert it is a view
        assert new_view.resource.body["view_of"] == api_ds.self
        assert new_view.resource.body["owner"] == project.self

        # Assert the variables are here
        for alias, v_type in self.FIXTURE_VARIABLES:
            assert new_view[alias].type == v_type

    def test_create_var_subset(self):
        api_ds = self._create_ds()
        scrunch_dataset = get_mutable_dataset(api_ds.body.id, self.site, editor=True)
        new_view = scrunch_dataset.views.create("Sub view", ["var1", "var2"])
        # Assert it is a view
        assert new_view.resource.body["view_of"] == api_ds.self

        # Only the desired variables are part of the view
        assert set(new_view.keys()) == {"var1", "var2"}

    def test_fetch_view_names(self):
        api_ds = self._create_ds()
        scrunch_dataset = get_mutable_dataset(api_ds.body.id, self.site, editor=True)
        view1 = scrunch_dataset.views.create("view 1", None)
        view2 = scrunch_dataset.views.create("view 2", ["var1", "var2"])

        assert scrunch_dataset.views.all_names() == {view1.name, "view 2"}

        fetched_view = scrunch_dataset.views.get_by_name("view 2")
        assert fetched_view.id == view2.id

