import mock
from unittest import TestCase

import pytest
from scrunch.datasets import Dataset


categories = [
    {"id": 3, "name": "Hipsters", "numeric_value": None, "missing": False},
    {"id": 1, "name": "Techies", "numeric_value": None, "missing": False},
    {"id": 2, "name": "Yuppies", "numeric_value": None, "missing": False}]

rules = ['gender == 1', 'gender == 2']


class TestCreateCategorical(TestCase):

    ds_url = 'http://test.crunch.io/api/datasets/123/'

    def test_create_categorical_with_missing(self):
        var_id = '0001'
        var_alias = 'gender'
        var_type = 'categorical'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)

        # Mocking setup.
        def _get(*args):
            if args[0] == 'id':
                return var_id
            if args[0] == 'alias':
                return var_alias
            if args[0] == 'type':
                return var_type
            if args[0] == 'is_subvar':
                return False
            return args[0]

        _var_mock = mock.MagicMock()
        _var_mock.entity.self = var_url
        _var_mock.__getitem__.side_effect = _get
        _var_mock.get.side_effect = _get

        class CrunchPayload(dict):
            def __getattr__(self, item):
                if item == 'payload':
                    return self
                else:
                    return self[item]

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return CrunchPayload({
                    'metadata': {
                        var_alias: _var_mock
                    }
                })
            return CrunchPayload()

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.__class__ = Dataset
        ds.create_categorical = Dataset.create_categorical
        ds.session.get.side_effect = _session_get

        ds.create_categorical(ds, categories, rules, 'name', 'alias', 'description')
        call = ds.variables.create.call_args_list[0][0][0]
        payload = {
          "element": "shoji:entity",
          "body": {
            "expr": {
              "function": "case",
              "args": [
                {
                  "column": [
                    3,
                    1,
                    2,
                    -1
                  ],
                  "type": {
                    "value": {
                      "class": "categorical",
                      "categories": [
                        {
                          "id": 3,
                          "numeric_value": None,
                          "missing": False,
                          "name": "Hipsters"
                        },
                        {
                          "id": 1,
                          "numeric_value": None,
                          "missing": False,
                          "name": "Techies"
                        },
                        {
                          "id": 2,
                          "numeric_value": None,
                          "missing": False,
                          "name": "Yuppies"
                        },
                        {
                          "name": "No Data",
                          "numeric_value": None,
                          "missing": True,
                          "id": -1
                        }
                      ]
                    }
                  }
                },
                {
                  "function": "==",
                  "args": [
                    {
                      "variable": "http://test.crunch.io/api/datasets/123/variables/0001/"
                    },
                    {
                      "value": 1
                    }
                  ]
                },
                {
                  "function": "==",
                  "args": [
                    {
                      "variable": "http://test.crunch.io/api/datasets/123/variables/0001/"
                    },
                    {
                      "value": 2
                    }
                  ]
                }
              ]
            },
            "description": "description",
            "name": "name",
            "alias": "alias"
          }
        }
        assert call == payload

    def test_create_categorical_raises_valueerror(self):
        sess = mock.MagicMock()
        ds = Dataset(sess)
        with pytest.raises(ValueError) as err:
            categories = [
                {'id': 2, 'name': 'Normal Users', 'numeric_value': 2, 'missing': False},
                {'id': 3, 'name': 'Hipsters', 'numeric_value': 3, 'missing': False},
                {'id': 32767, 'name': 'Unknown', 'numeric_value': None, 'missing': True}
            ]

            rules = [
                'operating_system in ("Linux", "Solaris", "Minix", "FreeBSD", "NetBSD")',
                'operating_system == "Windows"',
                'operating_system == "MacOS"',
                'missing(operating_system)'
            ]

            new_var = ds.create_categorical(
                categories=categories,
                rules=rules,
                name='Operating System Users',
                alias='operating_system_users',
                description='Type of Operating System Users'
            )

        assert str(err.value) == \
               'Amount of rules should match categories (or categories -1)'

