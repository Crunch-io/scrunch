import six

from pycrunch.cubes import fetch_cube, count
from scrunch.datasets import Variable
from scrunch.expressions import parse_expr, process_expr
from cr.cube.crunch_cube import CrunchCube


def crtabs(dataset, variables, weight=None, filter=None, **measures):
    """Return CrunchCube representation of crosstab.

    :param dataset: Dataset shoji object
    :param variables: List of variable urls, names or aliases
    :param weight: Scrunch variable instance, alias or url
    :param filter: Scrunch expression filter
    """
    variables = prepare_variables(variables)
    if weight:
        if isinstance(weight, Variable):
            weight = weight.url
        elif isinstance(weight, six.string_types):
            if weight in dataset.keys():
                weight = dataset[weight].url
        else:
            raise KeyError('{} not found in dataset'.format(weight))
    dataset = dataset.resource
    if filter:
        filter = process_expr(parse_expr(filter), dataset)
    return CrunchCube(fetch_cube(
        dataset, variables, count=count(), weight=weight, filter=filter, **measures))


def prepare_variables(variables):
    return [
        v.url if type(v) is Variable else v
        for v in variables
    ]
