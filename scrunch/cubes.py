from pycrunch.cubes import fetch_cube, count
from scrunch.datasets import Variable
from cr.cube.crunch_cube import CrunchCube


def crtabs(dataset, variables):
    """Return CrunchCube representation of crosstab.

    :param dataset: Dataset shoji object
    :param variables: List of variable urls, names or aliases
    """
    dataset = dataset.resource
    variables = prepare_variables(variables)
    return CrunchCube(fetch_cube(dataset, variables, count=count()))


def prepare_variables(variables):
    return [
        v.url if type(v) is Variable else v
        for v in variables
    ]
