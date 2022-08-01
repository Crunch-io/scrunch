# coding: utf-8
import os
import pytest
from unittest import TestCase

from scrunch import connect
from scrunch.scripts import ScriptExecutionError
from scrunch.mutable_dataset import get_mutable_dataset


HOST = os.environ["SCRUNCH_HOST"]
username = os.environ["SCRUNCH_USER"]
password = os.environ["SCRUNCH_PASS"]

site = connect(username, password, HOST)
assert site is not None, "Unable to connect to %s" % HOST

as_entity = lambda b: {
    "element": "shoji:entity",
    "body": b
}


class TestScripts(TestCase):
    def _create_ds(self):
        ds = site.datasets.create(as_entity({"name": "test_script"})).refresh()
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
        scrunch_dataset = get_mutable_dataset(ds.body.id, site, editor=True)
        script = """
        RENAME pk TO varA;

        CHANGE TITLE IN varA WITH "Variable A";
        """
        scrunch_dataset.scripts.execute(script)
        variable.refresh()
        assert variable.body["alias"] == "varA"
        assert variable.body["name"] == "Variable A"
        ds.delete()

    def test_handle_error(self):
        ds, variable = self._create_ds()
        scrunch_dataset = get_mutable_dataset(ds.body.id, site)
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
        scrunch_dataset = get_mutable_dataset(ds.body.id, site)
        s1 = "RENAME pk TO varA;"
        s2 = 'CHANGE TITLE IN varA WITH "Variable A";'

        scrunch_dataset.scripts.execute(s1)
        scrunch_dataset.scripts.execute(s2)

        scrunch_dataset.scripts.revert_to(script_number=0)  # Reverted
        variable.refresh()
        assert variable.body["name"] == "pk"
        ds.delete()

    def test_fetch_all_and_collapse(self):
        raise self.skipTest("Collapse is 504ing in the server.")
        ds, variable = self._create_ds()
        scrunch_dataset = get_mutable_dataset(ds.body.id, site)
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
        ds.delete()
