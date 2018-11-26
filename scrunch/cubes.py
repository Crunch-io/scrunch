import six

from pycrunch.cubes import fetch_cube, count
from scrunch.datasets import Variable
from scrunch.expressions import parse_expr, process_expr
from cr.cube.crunch_cube import CrunchCube


def variable_to_url(variable, ds):
    """
    Get a variable alias, instance or url and return
    the url
    """
    if isinstance(variable, Variable):
        return variable.url
    elif isinstance(variable, six.string_types):
        if variable in ds.keys():
            return ds[variable].url
    return variable


def crtabs(dataset, variables, weight=None, filter=None, **measures):
    """Return CrunchCube representation of crosstab.

    :param dataset: Dataset instance
    :param variables: List of variable urls, aliases or Variable instances
    :param weight: Scrunch variable instance, alias or url
    :param filter: Scrunch filter expression
    """
    variables = [variable_to_url(var, dataset) for var in variables]
    if weight:
        weight = variable_to_url(weight, dataset)
    if filter:
        filter = process_expr(parse_expr(filter), dataset.resource)
    return CrunchCube(fetch_cube(
        dataset.resource, variables, count=count(), weight=weight, filter=filter, **measures))
