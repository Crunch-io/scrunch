import pytest
from mock import patch, MagicMock
from pycrunch.cubes import fetch_cube, count
from scrunch.cubes import crtabs, prepare_variables
from scrunch.datasets import Variable


def test_prepare_variables(variables_fixture):
    """Test if urls are extracted from variables."""
    urls, variables = variables_fixture
    assert prepare_variables(variables) == urls


@patch('scrunch.cubes.fetch_cube')
def test_crtabs_passes_string_arguments(mock_fetch_cube, variables_fixture):
    """Test if url strings are passed to fetch_cube."""
    fake_ds = MagicMock(resource=MagicMock())
    mock_fetch_cube.return_value = {'result': {}}
    urls, _ = variables_fixture
    crtabs(fake_ds, urls)
    mock_fetch_cube.assert_called_once_with(fake_ds.resource, urls, count=count())


@patch('scrunch.cubes.fetch_cube')
def test_crtabs_passes_urls_for_variables(mock_fetch_cube, variables_fixture):
    """Test if urls are from variables are passed to fetch_cube."""
    fake_ds = MagicMock(resource=MagicMock())
    mock_fetch_cube.return_value = {'result': {}}
    urls, variables = variables_fixture
    crtabs(fake_ds, variables)
    mock_fetch_cube.assert_called_once_with(fake_ds.resource, urls, count=count())


@pytest.fixture(params=[
    ['fake url 1', 'fake url 2']
])
def variables_fixture(request):
    urls = request.param
    variables = [Variable(MagicMock(entity_url=url), MagicMock()) for url in urls]
    return urls, variables
