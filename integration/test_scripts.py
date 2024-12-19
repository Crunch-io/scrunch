# coding: utf-8
import os
from datetime import datetime

import pytest

from pycrunch.shoji import as_entity, wait_progress, TaskError

from scrunch.datasets import Project
from scrunch.helpers import shoji_entity_wrapper
from scrunch.scripts import ScriptExecutionError
from scrunch.mutable_dataset import get_mutable_dataset
from fixtures import BaseIntegrationTestCase


@pytest.mark.skipif(os.environ.get("LOCAL_INTEGRATION") is None, reason="Do not run this test during CI/CD")
class TestSystemScripts(BaseIntegrationTestCase):
    def new_project(self, name):
        res = self.site.projects.create(shoji_entity_wrapper({
            "name": name + datetime.now().strftime("%Y%m%d%H%M%S")
        })).refresh()
        return Project(res)

    def test_define_view_strict_subvariable_syntax(self):
        proj = self.site.projects.create(as_entity({"name": "foo"}))
        ds = self.site.datasets.create(as_entity(
            {"name": "test_dataset_script", "project": proj.self}
        )).refresh()
        categories = [
            {"id": 2, "name": "Home"},
            {"id": 3, "name": "Work"},
            {"id": -1, "name": "No Data", "missing": True},
        ]
        subvariables = [
            {"alias": "cat", "name": "Cat"},
            {"alias": "dog", "name": "Dog"},
            {"alias": "bird", "name": "Bird"},
        ]

        ds.variables.create(
            as_entity(
                dict(
                    alias="pets",
                    name="Pets",
                    type="categorical_array",
                    categories=categories,
                    subvariables=subvariables,
                    values=[[2, 3, 3], [3, 3, 2], [2, -1, 3], [3, 2, -1]],
                )
            )
        )
        ds.variables.create(
            as_entity(
                dict(
                    alias="pets_2",
                    name="Pets 2",
                    type="categorical_array",
                    categories=categories,
                    subvariables=subvariables,
                    values=[[2, 3, 3], [3, 3, 2], [2, -1, 3], [3, 2, -1]],
                )
            )
        )
        script_body = """
                        DEFINE VIEW FROM DATASET_ID(`{}`)
                        VARIABLES pets, pets_2
                        NAME "My view";
                        """.format(ds.body.id)

        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        project = self.new_project("test_view_strict_subvariable")
        project.move_here([scrunch_dataset])
        resp = project.execute(script_body, strict_subvariable_syntax=True)
        wait_progress(resp, self.site.session)
        view = scrunch_dataset.views.get_by_name("My view")
        assert view.project.name == project.name

    def test_define_view_strict_subvariable_syntax_error(self):
        proj = self.site.projects.create(as_entity({"name": "foo"}))
        ds = self.site.datasets.create(as_entity(
            {"name": "test_dataset_script_false", "project": proj.self}
        )).refresh()
        categories = [
            {"id": 2, "name": "Home"},
            {"id": 3, "name": "Work"},
            {"id": -1, "name": "No Data", "missing": True},
        ]
        subvariables = [
            {"alias": "cat", "name": "Cat"},
            {"alias": "dog", "name": "Dog"},
            {"alias": "bird", "name": "Bird"},
        ]

        ds.variables.create(
            as_entity(
                dict(
                    alias="pets",
                    name="Pets",
                    type="categorical_array",
                    categories=categories,
                    subvariables=subvariables,
                    values=[[2, 3, 3], [3, 3, 2], [2, -1, 3], [3, 2, -1]],
                )
            )
        )
        ds.variables.create(
            as_entity(
                dict(
                    alias="pets_2",
                    name="Pets 2",
                    type="categorical_array",
                    categories=categories,
                    subvariables=subvariables,
                    values=[[2, 3, 3], [3, 3, 2], [2, -1, 3], [3, 2, -1]],
                )
            )
        )
        script_body = """
                                DEFINE VIEW FROM DATASET_ID(`{}`)
                                VARIABLES pets, pets_2
                                NAME "My view";
                                """.format(ds.body.id)

        try:
            scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
            project = self.new_project("test_view_strict_subvariable_false")
            project.move_here([scrunch_dataset])
            resp = project.execute(script_body)
            with pytest.raises(TaskError) as err: 
                wait_progress(resp, self.site.session)
            err_value = err.value[0]
            err_value["type"] == "script:validation"
            err_value["description"] == "Errors processing the script"
            err_value["resolutions"][0]["message"] == "The following subvariables: bird, cat, dog exist in multiple arrays: pets, pets_2"
        finally:
            ds.delete()
            project.delete()


class TestDatasetScripts(BaseIntegrationTestCase):
    def _create_ds(self):
        project = self.site.projects.create(as_entity({"name": "foo"}))
        ds = self.site.datasets.create(as_entity({"name": "test_script", "project": project.self})).refresh()
        variable = ds.variables.create(
            as_entity(
                {
                    "name": "pk",
                    "alias": "pk",
                    "type": "numeric",
                }
            )
        )
        return ds, variable

    def test_execute(self):
        ds, variable = self._create_ds()
        try:
            scrunch_dataset = get_mutable_dataset(ds.body.id, self.site, editor=True)
            script = """
            RENAME pk TO varA;

            CHANGE TITLE IN varA WITH "Variable A";
            """
            scrunch_dataset.scripts.execute(script)
            variable.refresh()
            assert variable.body["alias"] == "varA"
            assert variable.body["name"] == "Variable A"
        finally:
            ds.delete()

    def test_handle_error(self):
        ds, variable = self._create_ds()
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        script = """BAD-RENAME pk TO varA;"""  # Bad syntax script
        with pytest.raises(ScriptExecutionError) as err:
            scrunch_dataset.scripts.execute(script)

        variable.refresh()
        assert variable.body["alias"] == "pk"  # Unchanged
        resolutions = err.value.resolutions[0]
        assert resolutions["column"] == 1
        assert resolutions["line"] == 1
        assert resolutions["command"] == 1
        assert resolutions["message"] == "Invalid command: BAD"

        # Script big enough to trigger async validation
        async_script = ["""BAD-RENAME pk TO varA;"""] * 50000
        async_script = "\n".join(async_script)
        assert len(async_script) > 2 ** 20  # This is the threshold for async
        with pytest.raises(ScriptExecutionError) as err:
            scrunch_dataset.scripts.execute(async_script)
        assert len(err.value.resolutions) == 50000  # All lines raised error
        resolutions = err.value.resolutions[0]
        assert resolutions["column"] == 1
        assert resolutions["line"] == 1
        assert resolutions["command"] == 1
        assert resolutions["message"] == "Invalid command: BAD"
        ds.delete()

    def test_revert_script(self):
        ds, variable = self._create_ds()
        scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
        s1 = "RENAME pk TO varA;"
        s2 = 'CHANGE TITLE IN varA WITH "Variable A";'

        scrunch_dataset.scripts.execute(s1)
        scrunch_dataset.scripts.execute(s2)

        scrunch_dataset.scripts.revert_to(script_number=0)  # Reverted
        variable.refresh()
        assert variable.body["name"] == "pk"
        ds.delete()

    @pytest.mark.skip(reason="Collapse is 504ing in the server.")
    def test_fetch_all_and_collapse(self):
        ds, variable = self._create_ds()
        try:
            scrunch_dataset = get_mutable_dataset(ds.body.id, self.site)
            s1 = "RENAME pk TO varA;"
            s2 = 'CHANGE TITLE IN varA WITH "Variable A";'

            scrunch_dataset.scripts.execute(s1)
            scrunch_dataset.scripts.execute(s2)

            r = scrunch_dataset.scripts.all()
            assert len(r) == 2
            assert r[0].body["body"] == s1
            assert r[1].body["body"] == s2

            scrunch_dataset.scripts.collapse()

            r = scrunch_dataset.scripts.all()
            assert len(r) == 1
        finally: 
            ds.delete()
