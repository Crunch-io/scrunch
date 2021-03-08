# coding: utf-8
import os
import pytest
from unittest import TestCase

from scrunch import connect
from scrunch.mutable_dataset import get_mutable_dataset


HOST = os.environ["SCRUNCH_HOST"]
username = os.environ["SCRUNCH_USER"]
password = os.environ["SCRUNCH_PASS"]

print("CREDS", username, password, HOST)

site = connect(username, password, HOST)
assert site is not None, "Unable to connect to %s" % HOST

as_entity = lambda b: {
    "element": "shoji:entity",
    "body": b
}


class TestSripts(TestCase):
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
    #
    # def test_execute(self):
    #     ds, variable = self._create_ds()
    #     scrunch_dataset = get_mutable_dataset(ds.body.id, site)
    #     script = """
    #     RENAME pk TO varA;
    #
    #     CHANGE TITLE IN varA WITH "Variable A";
    #     """
    #     scrunch_dataset.scripts.execute(script)
    #     variable.refresh()
    #     assert variable.body["alias"] == "varA"
    #     assert variable.body["name"] == "Variable A"
    #
    # def test_handle_error(self):
    #     assert True
    #
    # def test_revert_script(self):
    #     assert True

    def test_fetch_all_and_collapse(self):
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

        # r = scrunch_dataset.scripts.all()
        # assert len(r) == 2
        # assert r[0].body["body"] == """"""
        ds.delete()
