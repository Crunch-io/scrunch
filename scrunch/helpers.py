import requests
import six
from datetime import datetime

if six.PY2:  # pragma: no cover
    from urlparse import urljoin
else:
    from urllib.parse import urljoin


# ID's for default categories
SELECTED_ID = 1
NOT_SELECTED_ID = 2
NO_DATA_ID = -1


DEFAULT_MULTIPLE_RESONSE_CATEGORIES = [
    {'id': SELECTED_ID, 'name': 'Selected', 'missing': False, 'numeric_value': None, 'selected': True},
    {'id': NOT_SELECTED_ID, 'name': 'Not selected', 'missing': False, 'numeric_value': None, 'selected': False},
]


class ReadOnly(object):
    """
    class for protecting undesired writes to attributes
    """
    def __init__(self, resource):
        # need to call parent to make sure we call other mixin's __init__
        object.__setattr__(self, "resource", resource)
        object.__setattr__(self, "url", resource.self)

    def __setattr__(self, attr, value):
        if attr in self._IMMUTABLE_ATTRIBUTES:
            raise AttributeError(
                "Can't edit attibute '%s'" % attr)
        if attr in self._MUTABLE_ATTRIBUTES:
            raise AttributeError('use the edit() method for '
                                 'mutating attributes')
        object.__setattr__(self, attr, value)


def is_relative_url(url):
    return url.startswith(('.', '/'))


def abs_url(expr, base_url):
    """
    Converts an expression that may contain relative references to variable
     URLs into absolute URLs.

    This is necessary when using the derivation expression from a variable
    entity endpoint and sending it back to the variable catalog endpoint.
    """
    if isinstance(expr, dict):
        for k in expr:
            if k == 'variable':
                if is_relative_url(expr[k]):
                    expr[k] = urljoin(base_url, expr[k])
            elif isinstance(expr[k], dict):
                expr[k] = abs_url(expr[k], base_url)
            elif isinstance(expr[k], list):
                expr[k] = [abs_url(xpr, base_url) for xpr in expr[k]]
    elif isinstance(expr, list):
        expr = [abs_url(xpr, base_url) for xpr in expr]
    return expr


def subvar_alias(parent_alias, response_id):
    return '%s_%d' % (parent_alias, response_id)


def download_file(url, filename):
    if url.startswith('file://'):
        # Result is in local filesystem (for local development mostly)
        import shutil
        shutil.copyfile(url.split('file://', 1)[1], filename)
    else:
        r = requests.get(url, stream=True)
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:   # filter out keep-alive new chunks
                    f.write(chunk)
    return filename


def get_else_case(case, responses):
    """
    When creating a categorical like:
        responses=[
            {'id': 1, 'name': '30', 'case': 'age == 30'}
            {'id': 2, 'name': '40', 'case': 'age == 40'}
            {'id': 3, 'name': 'Other', 'case': 'else'}
        ]
    build the case for else: 'not (age==30) and not (age==40)'
    """
    if case == 'else':
        case = ' and '.join(
            ['not({})'.format(_case['case']) for _case in responses if _case['case'] != 'else']
        )
    return case


def else_case_not_selected(case, responses, missing_case):
    """
    When creating a multiple response through derive_multiple_response with:
        categories = [
            {
                'id': 1,
                'name': 'Coke',
                'case': 'q1 in [1]',
                'missing_case': 'missing(q1)'
            },
            {
                'id': 2,
                'name': 'Pepsi',
                'case': 'q1 in [2]',
                'missing_case': 'missing(q1)'
            },
            {
                'id': 3,
                'name': 'Other',
                'case': 'else',
                'missing_case': 'missing(screener4)'
            }]
    In this case, the else case needs to be manually built in the form:
        - not selected: '((q1 in [1]) or (q1 in [2]) and not (missing(screener4)))'
    """
    if case == 'else' and missing_case:
        missing = ' or '.join(
            ['({})'.format(_case['case']) for _case in responses if _case['case'] != 'else']
        )
        missing = '({})'.format(missing)
        missing += ' and not ({}) '.format(missing_case)
        return missing
    return None


def validate_categories(categories):
    """
    Categories can be defined as simple as:
        categories=[
            {'id': 1, 'name': 'Yes', 'selected': True},
            {'id': 2, 'name': 'No'},
            {'id': 3, 'name': 'Maybe'},
            {'id': 4, 'name': 'Missing', 'missing': True}
        ]
    This method takes care of validating that only 1 is selected
    and to fill the definition above to match the API requirement:
        categories=[
            {'id': 1, 'name': 'Yes', 'missing': False, 'numeric_value': None, 'selected': True},
            {'id': 2, 'name': 'No', 'missing': False, 'numeric_value': None, 'selected': False},
            {'id': 3, 'name': 'Maybe', 'missing': False, 'numeric_value': None, 'selected': False},
            {'id': 4, 'name': 'Missing', 'missing': True, 'numeric_value': None, 'selected': False}
        ]
    """
    defaults = {'missing': False, 'numeric_value': None, 'selected': False}
    selected_count = 0
    for category in categories:
        if category.get('selected'):
            selected_count += 1
        if not category.get('id'):
            raise ValueError('An "id" must be provided to all categories')
        if not category.get('name'):
            raise ValueError('A "name" must be provided to all categories')
    if selected_count > 1 or selected_count == 0:
        raise ValueError('Categories must define one category as selected')
    _categories = []
    for category in categories:
        default = defaults.copy()
        default.update(category)
        _categories.append(default)
    return _categories


def case_expr(rules, name, alias, categories=DEFAULT_MULTIPLE_RESONSE_CATEGORIES):
    """
    Given a set of rules, return a `case` function expression to create a
    variable.
    """
    expression = {
        'references': {
            'name': name,
            'alias': alias,
        },
        'function': 'case',
        'args': [{
            'column': [category['id'] for category in categories],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': categories
                }
            }
        }]
    }
    for rule in rules:
        expression['args'].append(rule)
    return expression


def _validate_category_rules(categories, rules):
    """
    validate that rules and categories are of same size
    """

    if not ((len(categories) - 1) <= len(rules) <= len(categories)):
        raise ValueError(
            'Amount of rules should match categories (or categories -1)'
        )


def shoji_view_wrapper(value, **kwargs):
    """
    receives a dictionary and wraps its content on a body keyed dictionary
    with the appropiate shoji:<class> attribute
    """
    payload = {
        'element': 'shoji:view',
        'value': value
    }
    payload.update(**kwargs)
    return payload


def shoji_entity_wrapper(body, **kwargs):
    """
    receives a dictionary and wraps its content on a body keyed dictionary
    with the appropiate shoji:<class> attribute
    """
    payload = {
        'element': 'shoji:entity',
        'body': body
    }
    payload.update(**kwargs)
    return payload


def shoji_catalog_wrapper(index, **kwargs):
    """
    receives a dictionary and wraps its content on a body keyed dictionary
    with the appropiate shoji:<class> attribute
    """
    payload = {
        'element': 'shoji:catalog',
        'index': index
    }
    payload.update(**kwargs)
    return payload


def valid_categorical_date(date_str):
    """
    Categories accept a `date` attribute that needs to be a valid ISO8601 date.
    In order to keep dependencies reduced (no dateutil) and Python2x support,
    we will support a limited set of simple date formats.
    """
    valid_date_masks = [
        "%Y",
        "%Y-%m",
        "%Y-%m-%d",
    ]
    for mask in valid_date_masks:
        try:
            datetime.strptime(date_str, mask)
            return True
        except ValueError:
            # Did not validate for this mask
            continue
    return False

