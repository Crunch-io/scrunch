import re

import six

VARIABLE_URL_REGEX = re.compile(
    r"^(http|https):\/\/(.*)\/api\/datasets\/([\w\d]+)\/variables\/([\w\d]+)"
    r"(\/subvariables\/([\w\d]*))?\/?$"
)


def validate_variable_url(url):
    """
    Checks if a given url matches the variable url regex or not.
    """
    return VARIABLE_URL_REGEX.match(url)


def combinations_from_map(map, categories, missing):
    missing = missing if isinstance(missing, list) else [missing]
    combinations = [{
        'id': cat_id,
        'name': categories.get(cat_id, "Category %s" % cat_id),
        'missing': cat_id in missing,
        'combined_ids': combined_ids if isinstance(combined_ids, (list, tuple)) else [combined_ids]
    } for cat_id, combined_ids in sorted(six.iteritems(map))]
    return combinations


def combine_categories_expr(var_alias, combinations):
    return {
        'function': 'combine_categories',
        'args': [{
            'var': var_alias
        }, {
            'value': combinations
        }]
    }
