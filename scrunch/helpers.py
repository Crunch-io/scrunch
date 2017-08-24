import requests
import six

if six.PY2:  # pragma: no cover
    from urlparse import urljoin
else:
    from urllib.parse import urljoin


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


def case_expr(rules, name, alias):
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
            'column': [1, 2],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': [
                        {'id': 1, 'name': 'Selected', 'missing': False, 'numeric_value': None, 'selected': True},
                        {'id': 2, 'name': 'Not selected', 'missing': False, 'numeric_value': None, 'selected': False},
                    ]
                }
            }
        }]
    }
    expression['args'].append(rules)
    return expression


def _validate_category_rules(categories, rules):
    """
    validate that rules and categories are of same size
    """

    if not ((len(categories) - 1) <= len(rules) <= len(categories)):
        raise ValueError(
            'Amount of rules should match categories (or categories -1)'
        )


def shoji_entity_wrapper(payload):
    """
    receives a dictionary and wraps its content on a body keyed dictionary
    with the appropiate shoji:<class> attribute
    """
    return {
        'element': 'shoji:entity',
        'body': payload
    }
