from mock import patch
from unittest import TestCase

from pycrunch.cubes import count
from scrunch.streaming_dataset import StreamingDataset
from scrunch.tests.test_datasets import TestDatasetBase
from scrunch.cubes import crtabs, variable_to_url


class TestCubes(TestDatasetBase, TestCase):
    @patch("scrunch.cubes.fetch_cube")
    def test_crtabs_passes_string_arguments(self, mock_fetch_cube):
        """
        Test url aliases are converted to urls
        """
        ds_mock = self._dataset_mock()
        ds = StreamingDataset(ds_mock)
        variables = ["var1_alias", "var2_alias"]
        urls = [variable_to_url(var, ds) for var in variables]
        crtabs(dataset=ds, variables=variables)
        mock_fetch_cube.assert_called_once_with(
            ds.resource, urls, count=count(), weight=None, filter=None
        )

    @patch("scrunch.cubes.fetch_cube")
    def test_weight_to_url(self, mock_fetch_cube):
        """
        Test weight alias is converted to url
        """
        ds_mock = self._dataset_mock()
        ds = StreamingDataset(ds_mock)
        variables = ["var1_alias", "var2_alias"]
        weight_url = variable_to_url("var3_alias", ds)
        urls = [variable_to_url(var, ds) for var in variables]
        crtabs(dataset=ds, variables=variables, weight="var3_alias")
        mock_fetch_cube.assert_called_once_with(
            ds.resource, urls, count=count(), weight=weight_url, filter=None
        )

    @patch("scrunch.cubes.fetch_cube")
    def test_pass_filter_expression(self, mock_fetch_cube):
        ds_mock = self._dataset_mock()
        ds = StreamingDataset(ds_mock)
        variables = ["var1_alias", "var2_alias"]
        urls = [variable_to_url(var, ds) for var in variables]
        crtabs(dataset=ds, variables=variables, filter_="var1_alias > 1")
        processed_filter = {
            "function": ">",
            "args": [
                {
                    "variable": "https://test.crunch.io/api/datasets/123456/variables/0001/"
                },
                {"value": 1},
            ],
        }
        mock_fetch_cube.assert_called_once_with(
            ds.resource, urls, count=count(), filter=processed_filter, weight=None
        )

