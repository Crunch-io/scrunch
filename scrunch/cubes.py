from pycrunch.cubes import fetch_cube, count
from cr.cube.crunch_cube import CrunchCube


def crtabs(dataset, variables):
    """Return CrunchCube representation of crosstab.

    :param dataset: Dataset shoji object
    :param variables: List of variable urls, names or aliases
    """
    dataset = dataset.resource
    return CrunchCube(fetch_cube(dataset, variables, count=count()))
