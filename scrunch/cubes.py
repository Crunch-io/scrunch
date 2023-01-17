import six

from pycrunch.cubes import fetch_cube, count
from scrunch.datasets import Variable
from scrunch.expressions import parse_expr, process_expr

from cr.cube.cube import Cube


def variable_to_url(variable, dataset):
    """
    Get a variable alias, instance or url and return
    the url
    """
    if isinstance(variable, Variable):
        return variable.url
    elif isinstance(variable, six.string_types):
        if variable in dataset.keys():
            return dataset[variable].url
    return variable


def crtabs(dataset, variables, weight=None, filter_=None, transforms=None, **measures):
    """Return Cube representation of crosstab.

    :param dataset: Dataset instance
    :param variables: List of variable urls, aliases or Variable instances
    :param weight: Scrunch variable instance, alias or url
    :param filter_: Scrunch filter expression
    :param transforms: cr.cube transforms dictionary
    """
    variables = [variable_to_url(var, dataset) for var in variables]
    if weight is not None:
        weight = variable_to_url(weight, dataset)
    if filter_ is not None:
        filter_ = process_expr(parse_expr(filter_), dataset.resource)

    # if six.PY2:
    #     raise DeprecationWarning("Crunch Cube isn't supported in Python 2.x")

    return Cube(
        fetch_cube(
            dataset.resource,
            variables,
            count=count(),
            weight=weight,
            filter=filter_,
            **measures
        ),
        transforms=transforms,
    )
