import re

import six

from scrunch.helpers import subvar_alias

VARIABLE_URL_REGEX = re.compile(
    r"^(http|https):\/\/(.*)\/api\/datasets\/([\w\d]+)\/variables\/([\w\d]+)"
    r"(\/subvariables\/([\w\d]*))?\/?$"
)


def validate_variable_url(url):
    """
    Checks if a given url matches the variable url regex or not.
    """
    return VARIABLE_URL_REGEX.match(url)


def responses_from_map(variable, response_map, cat_names, alias, parent_alias):
    subvars = variable.resource.subvariables.by('alias')

    # In python 2.7, range(...) returns a list, starting from python 3,
    # range is a python type
    if six.PY2:
        _supported_iterable_types = (list, tuple)
    else:
        _supported_iterable_types = (list, tuple, range)

    try:
        responses = [
            {
                'name': cat_names.get(response_id, "Response %s" % response_id),
                'alias': subvar_alias(alias, response_id),
                'combined_ids': [
                    subvars[subvar_alias(parent_alias, sv_alias)].entity_url
                    for sv_alias in (combined_ids if isinstance(combined_ids, _supported_iterable_types) else [combined_ids])
                ]
            } for response_id, combined_ids in sorted(six.iteritems(response_map))
        ]
    except KeyError:
        # This means we tried to combine a subvariable with ~id~ that does not
        # exist in the subvariables. Treat as bad input.
        raise ValueError("Unknown subvariables for variable %s" % parent_alias)
    return responses


def combinations_from_map(map, categories, missing):
    missing = missing if isinstance(missing, list) else [missing]
    combinations = [{
        'id': cat_id,
        'name': categories.get(cat_id, "Category %s" % cat_id),
        'missing': cat_id in missing,
        'combined_ids': combined_ids if isinstance(combined_ids, (list, tuple)) else [combined_ids]
    } for cat_id, combined_ids in sorted(six.iteritems(map))]
    return combinations


def combine_responses_expr(variable_url, responses):
    return {
        'function': 'combine_responses',
        'args': [{
            'variable': variable_url
        }, {
            'value': responses
        }]
    }


def combine_categories_expr(variable_url, combinations):
    return {
        'function': 'combine_categories',
        'args': [{
            'variable': variable_url
        }, {
            'value': combinations
        }]
    }
