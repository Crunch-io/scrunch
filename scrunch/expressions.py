"""
This module provides basic support for converting expression strings (defined
in a python-like DSL) into crunch expression objects.

For example, the expression 'disposition == 0 or exit_status == 0' would
be transformed by this module's parser into:

        {
            'function': 'or',
            'args': [
                {
                    'function': '==',
                    'args': [
                        {
                            'variable': 'disposition'
                        },
                        {
                            'value': 0
                        }
                    ]
                },
                {
                    'function': '==',
                    'args': [
                        {
                            'variable': 'exit_status'
                        },
                        {
                            'value': 0
                        }
                    ]
                }
            ]
        }

Its important to note that the expression objects produced by this module's
parser are not ready for being sent to crunch, as they refer to variables
by `alias` rather than by `variable_id` (which is a URL). So, this module
also provides a `process_expr` function that creates an expression object
ready for the crunch API.
"""

import ast
import copy

import six
import scrunch
from scrunch.variables import validate_variable_url

CRUNCH_FUNC_MAP = {
    'valid': 'is_valid',
    'missing': 'is_missing',
}

CRUNCH_METHOD_MAP = {
    'has_any': 'any',
    'has_all': 'all',
    'duplicates': 'duplicates',
    'has_count': 'has_count',
}

# according to http://docs.crunch.io/#function-terms
BINARY_FUNC_OPERATORS = [
    '+',
    '-',
    '*',
    '/',
    '//',
    '^',
    '%',
    '&',
    '|',
    '~',
]

COMPARISSON_OPERATORS = [
    '==',
    '!=',
    '=><=',
    '<',
    '>',
    '<=',
    '>=',
    '~=',
    'in',
    'and',
    'or',
    'not',
]

COMPARISSON_FUNCS = [

    # 'between',
    'all',
    'any',
]

BUILTIN_FUNCTIONS = []

NOT_IN = object()


def _nest(args, func):
    # for the moment we are just nesting and & or
    if func not in ['or', 'and']:
        return {
            'function': func,
            'args': args
        }
    if len(args) == 2:
        return {
            'function': func,
            'args': args
        }
    return {
        'function': func,
        'args': [args[0], _nest(args[1:], func)]
    }


def parse_expr(expr):

    def _parse(node, parent=None):
        obj = {}
        args = []
        op = None
        func_type = None

        if isinstance(node, ast.AST):
            # Get the current node fields.
            fields = list(ast.iter_fields(node))

            # "Terminal" nodes. Recursion ends with these guys.
            if isinstance(node, ast.Name):
                _id = fields[0][1]

                # A function identifier.
                if getattr(node, '_func_type', None) == 'function':
                    return _id

                # A variable identifier.
                return {
                    'variable': _id
                }
            elif isinstance(node, ast.Num) or isinstance(node, ast.Str):
                _val = fields[0][1]
                return {
                    'value': _val
                }
            elif isinstance(node, ast.Eq):
                return '=='
            elif isinstance(node, ast.NotEq):
                return '!='
            elif isinstance(node, ast.Lt):
                return '<'
            elif isinstance(node, ast.LtE):
                return '<='
            elif isinstance(node, ast.Gt):
                return '>'
            elif isinstance(node, ast.GtE):
                return '>='
            elif isinstance(node, ast.In):
                return 'in'
            elif isinstance(node, ast.NotIn):
                return NOT_IN
            elif isinstance(node, ast.List) or isinstance(node, ast.Tuple):
                _list = fields[0][1]
                if not (all(isinstance(el, ast.Str) for el in _list) or
                        all(isinstance(el, ast.Num) for el in _list)):
                    # Only list-of-int or list-of-str are currently supported
                    raise ValueError

                return {
                    'value': [
                        getattr(el, 's', None) or getattr(el, 'n')
                        for el in _list
                    ]
                }
            elif isinstance(node, ast.Attribute) \
                    and isinstance(parent, ast.Call):
                # The variable.
                _id_node = fields[0][1]
                if not isinstance(_id_node, ast.Name):
                    raise ValueError
                _id = _parse(_id_node, parent=node)

                # The 'method'.
                method = fields[1][1]
                if method not in CRUNCH_METHOD_MAP.keys():
                    raise ValueError

                return _id, CRUNCH_METHOD_MAP[method]

            # "Non-terminal" nodes.
            else:
                for _name, _val in fields:
                    if not isinstance(node, ast.UnaryOp) and (
                            isinstance(_val, ast.BoolOp)
                            or isinstance(_val, ast.UnaryOp)
                            or isinstance(_val, ast.Compare)
                            or isinstance(_val, ast.Call)):
                        # Descend.
                        obj.update(_parse(_val, parent=node))
                    elif isinstance(_val, ast.And):
                        op = 'and'
                    elif isinstance(_val, ast.Or):
                        op = 'or'
                    elif isinstance(_val, ast.Not):
                        op = 'not'
                    elif isinstance(_val, ast.Mult):
                        op = '*'
                    elif isinstance(_val, ast.Add):
                        op = '+'
                    elif isinstance(_val, ast.Div):
                        op = '/'
                    elif isinstance(_val, ast.Sub):
                        op = '-'
                    elif _name == 'left':
                        left = _parse(_val, parent=node)
                        args.append(left)
                    elif _name == 'right':
                        right = _parse(_val, parent=node)
                        args.append(right)
                    elif _name == 'func' and isinstance(_val, ast.Attribute):
                        # Method-like call. Example:
                        #       variable.has_any([1,2])
                        func_type = 'method'
                        setattr(_val, '_func_type', func_type)
                        left, op = _parse(_val, parent=node)
                        args.append(left)
                    elif _name == 'func' and isinstance(_val, ast.Name):
                        # Function call. Example:
                        #       valid(birthyear, birthmonth)
                        func_type = 'function'
                        setattr(_val, '_func_type', func_type)
                        _id = _parse(_val, parent=node)
                        if _id not in CRUNCH_FUNC_MAP.keys():
                            raise ValueError
                        op = CRUNCH_FUNC_MAP[_id]
                    elif _name == 'ops':
                        if len(_val) != 1:
                            raise ValueError
                        op = _parse(_val[0], parent=node)
                    elif _name == 'comparators' or _name == 'args':  # right
                        if len(_val) == 0:
                            continue

                        if func_type == 'method':
                            if len(_val) > 1:
                                raise ValueError

                            if op == 'duplicates':
                                # No parameters allowed for 'duplicates'.
                                raise ValueError

                        for arg in _val:
                            right = _parse(arg, parent=node)

                            # For method calls, we only allow list-of-int
                            # parameters.
                            if _name == 'args' and func_type == 'method' \
                                    and op != 'has_count':
                                if 'value' not in right \
                                        or not isinstance(right['value'], list):
                                    raise ValueError

                            args.append(right)

                        if op == 'has_count':
                            if not isinstance(args[-1].get('value'), int):
                                raise ValueError

                    elif _name in ('keywords', 'starargs', 'kwargs') and _val:
                        # We don't support these in function/method calls.
                        raise ValueError
                    elif _name == 'operand' and isinstance(node, ast.UnaryOp):
                        right = _parse(_val, parent=node)
                        args.append(right)
                    elif isinstance(_val, list):
                        for arg in _val:
                            args.append(_parse(arg, parent=node))

                if op:
                    if op is NOT_IN:
                        # Special treatment for the `not in` operator.
                        obj = {
                            'function': 'not',
                            'args': [
                                {
                                    'function': 'in',
                                    'args': []
                                }
                            ]
                        }
                    elif op in CRUNCH_FUNC_MAP.values() \
                            and isinstance(args, list) and len(args) > 1:
                        obj = {
                            'function': 'and',
                            'args': []
                        }
                    else:
                        obj = {
                            'function': op,
                            'args': []
                        }

                if args and 'args' in obj:
                    if op is NOT_IN:
                        # Special treatment for the args in a `not in` expr.
                        obj['args'][0]['args'] = args
                    elif op in CRUNCH_FUNC_MAP.values() \
                            and isinstance(args, list) and len(args) > 1:
                        for arg in args:
                            obj['args'].append(
                                {
                                    'function': op,
                                    'args': [arg]
                                }
                            )
                    else:
                        obj = _nest(args, op)

        return obj

    if expr is None:
        return dict()

    return _parse(ast.parse(expr, mode='eval'))


def get_dataset_variables(ds):
    table_url = ds.fragments.table
    table = ds.session.get(table_url, params={'limit': 0}).payload

    # Build the variables dict, using `alias` as the key.
    variables = dict()
    for _id, var in table.metadata.items():
        var['id'] = _id
        variables[var['alias']] = var

        if var['type'] in ('categorical_array', 'multiple_response'):
            for i, subvar in enumerate(var.get('subreferences', [])):
                subvar['is_subvar'] = True
                subvar['id'] = var['subvariables'][i]
                subvar['parent_id'] = _id
                subvar['type'] = 'categorical'
                subvar['description'] = ''
                subvar['categories'] = copy.deepcopy(var['categories'])
                variables[subvar['alias']] = subvar

    return variables


def process_expr(obj, ds):
    """
    Given a Crunch expression object (or objects) and a Dataset entity object
    (i.e. a Shoji entity), this function returns a new expression object
    (or a list of new expression objects) with all variable aliases
    transformed into variable URLs, just as the crunch API needs them to be.
    """
    base_url = ds.self
    variables = get_dataset_variables(ds)

    def _process(obj, variables):
        op = None
        arrays = []
        values = []
        subvariables = []

        for key, val in obj.items():
            if isinstance(val, dict):
                obj[key] = _process(val, variables)
            elif isinstance(val, list) or isinstance(val, tuple):
                subitems = []
                for subitem in val:
                    if isinstance(subitem, dict):
                        subitem = _process(subitem, variables)
                        if 'subvariables' in subitem:
                            arrays.append(subitem.pop('subvariables'))
                        elif 'value' in subitem:
                            values.append(subitem)
                    subitems.append(subitem)
                obj[key] = subitems
            elif key == 'variable':
                var = variables.get(val)
                if var:
                    if var.get('is_subvar'):
                        obj[key] = '%svariables/%s/subvariables/%s/' \
                                   % (base_url, var['parent_id'], var['id'])
                    else:
                        obj[key] = '%svariables/%s/' % (base_url, var['id'])

                    if var['type'] in ('categorical_array', 'multiple_response'):
                        subvariables = [
                            '%svariables/%s/subvariables/%s/'
                            % (base_url, var['id'], subvar_id)
                            for subvar_id in var.get('subvariables', [])
                        ]
                else:
                    raise ValueError("Invalid variable alias '%s'" % val)
            elif key == 'function':
                op = val

        if subvariables:
            obj['subvariables'] = subvariables

        if arrays and op in ('any', 'all', 'is_valid', 'is_missing'):
            # Support for array variables.

            if len(arrays) != 1:
                raise ValueError

            real_op = 'in'
            expansion_op = 'or'
            if op == 'all':
                real_op = '=='
                expansion_op = 'and'
            elif op == 'is_valid':
                real_op = 'all_valid'
            elif op == 'is_missing':
                real_op = 'all_missing'

            if op in ('is_valid', 'is_missing'):
                if len(values) != 0:
                    raise ValueError

                # Just swap the op. Yep, that's it.
                obj['function'] = real_op
            else:
                if len(values) != 1:
                    raise ValueError

                subvariables = arrays[0]
                value = values[0]

                def _extract_id(url):
                    '''Extracts the id form the given url'''
                    return [
                        part for part in url.split('/') if part != ''
                    ][-1]

                subvariables_dict = {
                    _extract_id(subvar): subvar
                    for subvar in subvariables
                }

                # no expansion is needed if all arguments are subvariables,
                # instead we should just transform variable references to
                # their corresponding urls
                are_subvars = all(
                    subvar in value['value']
                    for subvar in subvariables_dict.keys()
                )

                if are_subvars:
                    value['value'] = [
                        subvariables_dict[v] for v in value['value']
                    ]
                else:
                    if op == 'all':
                        if len(value['value']) != 1:
                            raise ValueError
                        value['value'] = value['value'][0]

                    if len(subvariables) == 1:
                        obj['function'] = real_op
                        obj['args'][0] = {'variable': subvariables[0]}
                        obj['args'][1] = value
                    else:
                        obj = {
                            'function': expansion_op,
                            'args': []
                        }
                        args_ref = obj['args']
                        for i, subvar in enumerate(subvariables):
                            args_ref.append(
                                {
                                    'function': real_op,
                                    'args': [
                                        {'variable': subvar},
                                        value
                                    ]
                                }
                            )
                            if i < len(subvariables) - 2:
                                args_ref.append(
                                    {'function': expansion_op, 'args': []}
                                )
                                args_ref = args_ref[-1]['args']

        return obj

    if isinstance(obj, list):
        return [
            _process(copy.deepcopy(element), variables) for element in obj
        ]
    else:
        return _process(copy.deepcopy(obj), variables)


def prettify(expr, ds=None):
    """
    Translate the crunch expression dictionary to the string representation.

    :param expr: crunch expression
    :param ds: dataset instance
    :return: string representation of the expression
    """
    assert isinstance(expr, dict), "Dictionary is expected"

    operators = BINARY_FUNC_OPERATORS + COMPARISSON_OPERATORS
    methods = {m[1]: m[0] for m in CRUNCH_METHOD_MAP.items()}

    def _resolve_variable(var):
        is_url = validate_variable_url(var)

        if not is_url:
            return var
        elif not isinstance(ds, scrunch.datasets.Dataset):
            raise Exception(
                'Valid Dataset instance is required to resolve variable urls '
                'in the expression'
            )
        return ds.session.get(var).payload.body.alias

    def _resolve_variables(_expr):
        new_expr = dict(
            function=_expr['function'],
            args=[]
        )
        for arg in _expr['args']:
            if 'function' in arg:
                # arg is a function, resolve inner variables
                new_expr['args'].append(_resolve_variables(arg))
            elif 'variable' in arg:
                # arg is a variable, resolve
                new_expr['args'].append(
                    {'variable': _resolve_variable(arg['variable'])}
                )
            else:
                # arg is neither a variable or function, pass as is
                new_expr['args'].append(arg)
        return new_expr

    def _transform(f, args, nest=False):
        result = ''
        if f in operators:
            if len(args) == 1:
                result = '%s %s' % (f, args[0])
            else:
                op = ' %s ' % f
                result = op.join(str(x) for x in args)
        elif f in methods:
            result = '%s.%s(%s)' % (
                args[0], methods[f], ', '.join(str(x) for x in args[1:])
            )
        else:
            raise Exception('Unknown function "%s"' % f)

        if nest:
            result = '(%s)' % result

        return result

    def _quote_value(v):
        # escape the quotes from the string, also escape the backslash
        return "'{}'".format(
            v.replace("\\", "\\\\").replace("\'", "\\\'")
        )

    def _process(fragment, parent=None):
        _func = fragment.get('function')

        if _func is None:
            # This is not a function, but a plain argument

            if 'value' in fragment:
                # This argument is a value, not a variable
                value = fragment['value']

                if isinstance(value, six.string_types):
                    # Must escape single-quote from string value
                    value = _quote_value(value)

                return value

            return list(fragment.values())[0]

        args = [_process(arg, _func) for arg in fragment['args']]
        child_functions = [
            arg.get('function')
            for arg in fragment['args'] if arg.get('function') is not None
        ]
        has_child_and_or = 'or' in child_functions
        nest = parent is not None and (
            has_child_and_or
            or (parent == 'or' and len(child_functions) > 1)
            or _func == 'or'
        )
        return _transform(_func, args, nest=nest)

    return _process(_resolve_variables(expr))
