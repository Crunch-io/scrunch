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
from scrunch.helpers import is_number
from scrunch.variables import validate_variable_url

import sys

GT_PY_311 = sys.version_info[:2] >= (3, 11)

if six.PY2:
    from urllib import urlencode
else:
    from urllib.parse import urlencode

ARRAY_TYPES = ('categorical_array', 'multiple_response', 'numeric_array')

CRUNCH_FUNC_MAP = {
    'valid': 'is_valid',
    'missing': 'is_missing',
    'bin': 'bin',
    'selected': 'selected',
    'not_selected': 'not_selected',
}

CRUNCH_METHOD_MAP = {
    'any': 'any',
    'all': 'all',
    'duplicates': 'duplicates',
    'bin': 'bin',
    'selected': 'selected',
    'not_selected': 'not_selected',
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


def _nest(args, func, concatenator=None):
    """
    :param args: list of arguments that need nesting
    :param func: the function that applies to tuple of args
    :param concatenator: the concatenator or tuples, usually 'or' 'and'
    :return:
    """
    if not concatenator:
        # in case of is_missing and is_valid we need to concatenate
        # multiple arguments and nest them
        concatenator = func
    # for the moment we are just nesting and & or
    if func not in ['or', 'and', 'is_missing', 'is_valid'] or len(args) < 3:
        return {
            'function': concatenator,
            'args': args
        }
    return {
        'function': concatenator,
        'args': [args[0], _nest(args[1:], func, concatenator)]
    }


def unfold_list(_list):
    if any(isinstance(x, ast.Call) for x in _list):
        new_list = list()
        for e in _list:
            if isinstance(e, ast.Call):
                name = e.func.id
                if name == 'r':
                    try:
                        lower = e.args[0].n
                        upper = e.args[1].n
                        r_list = r(lower, upper)
                        for elem in r_list:
                            new_list.append(ast.Num(elem))
                    except Exception:
                        raise AttributeError(
                            "function 'r' needs 2 integer arguments")
                else:
                    return _list
            else:
                new_list.append(e)
        return new_list
    else:
        return _list


def r(lower, upper):
    return list(range(lower, upper + 1))


def parse_expr(expr):
    """
    Converts a text python-like expression into ZCL tree.

    :param expr: String with a python-like expression
    :return: Dictionary with a ZCL expression
    """

    def _var_term(_var_id):
        return {"var": _var_id}

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
                return _var_term(_id)
            elif isinstance(node, ast.Num) or isinstance(node, ast.Str):
                if isinstance(parent, ast.Call) \
                        and 'func' in parent._fields:
                    _id = fields[0][1]
                    return _var_term(_id)

                _val = fields[0][1]
                return {
                    'value': _val
                }
            elif isinstance(node, ast.Add):
                return '+'
            elif isinstance(node, ast.Sub):
                return '-'
            elif isinstance(node, ast.Mult):
                return '*'
            elif isinstance(node, ast.Div):
                return '/'
            elif isinstance(node, ast.FloorDiv):
                return '//'
            elif isinstance(node, ast.Pow):
                return '^'
            elif isinstance(node, ast.Mod):
                return '%'
            elif isinstance(node, ast.BitAnd):
                return '&'
            elif isinstance(node, ast.BitOr):
                return '|'
            elif isinstance(node, ast.Invert):
                return '~'
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
                # checks for special helper functions like `r`
                _list = unfold_list(_list)
                if all(isinstance(el, ast.Name) for el in _list):
                    # This is the case of `any([subvar_1, subvar_2])]
                    return {'column': [el.id for el in _list]}
                elif not (all(isinstance(el, ast.Str) for el in _list) or
                        all(isinstance(el, ast.Num) for el in _list)):
                    # Only list-of-int or list-of-str are currently supported
                    raise ValueError('Only list-of-int or list-of-str are currently supported')

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
                if not isinstance(_id_node, (ast.Name, ast.Subscript)):
                    msg = (
                        'calling methods of "{}" object not allowed, '
                        'variable name expected.'
                    ).format(type(_id_node).__name__)
                    raise SyntaxError(msg)

                _id = _parse(_id_node, parent=node)

                # The 'method'.
                method = fields[1][1]
                if method not in CRUNCH_METHOD_MAP.keys():
                    raise ValueError(
                        'unknown method "{}", valid methods are: [{}]'.format(
                            method,
                            ', '.join(CRUNCH_METHOD_MAP.keys())
                        )
                    )

                return _id, CRUNCH_METHOD_MAP[method]
            elif isinstance(node, ast.Subscript):
                # Handle the case of `array_alias[subvariable_alias]`
                # We will take the subvariable alias bit from the subscript
                # and return an object with the array and subvariable alias
                array_alias = dict(ast.iter_fields(fields[0][1]))["id"]
                if GT_PY_311:                
                    name_node = dict(ast.iter_fields(fields[1][1]))
                    subvariable_alias = name_node["id"]
                else:
                    name_node = dict(ast.iter_fields(fields[1][1]))["value"]
                    subscript_fields = dict(ast.iter_fields(name_node))
                    subvariable_alias = subscript_fields["id"]
                return {"var": array_alias, "axes": [subvariable_alias]}
            # "Non-terminal" nodes.
            else:
                for _name, _val in fields:
                    if not isinstance(node, ast.UnaryOp) and (
                            isinstance(_val, (ast.BoolOp, ast.UnaryOp, ast.Compare, ast.Call))):
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
                        #       variable.any([1,2])
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
                            raise ValueError(
                                'unknown method "{}", valid methods are: [{}]'.format(
                                    _id,
                                    ', '.join(CRUNCH_METHOD_MAP.keys())
                                )
                            )
                        op = CRUNCH_FUNC_MAP[_id]
                    elif _name == 'ops':
                        if len(_val) != 1:
                            raise ValueError('only one logical operator at a time')
                        op = _parse(_val[0], parent=node)
                    elif _name == 'comparators' or _name == 'args':  # right
                        if len(_val) == 0:
                            continue

                        if func_type == 'method':
                            if len(_val) > 1:
                                raise ValueError('1 argument expected, got {}'.format(len(_val)))

                            if op == 'duplicates':
                                # No parameters allowed for 'duplicates'.
                                raise ValueError('No parameters allowed for "duplicates"')

                        for arg in _val:
                            right = _parse(arg, parent=node)

                            # For method calls, we only allow list-of-int
                            # parameters.
                            if _name == 'args' and func_type == 'method':
                                right_val = right.get('value', right.get('column'))
                                if not isinstance(right_val, list):
                                    raise ValueError(
                                        'expected list, got "{}"'.format(
                                            type(right.get('value'))
                                        )
                                    )

                            args.append(right)

                    elif _name in ('keywords', 'starargs', 'kwargs') and _val:
                        # We don't support these in function/method calls.
                        raise ValueError('unsupported call with argument "{}"'.format(_name))
                    elif _name == 'operand' and isinstance(node, ast.UnaryOp):
                        right = _parse(_val, parent=node)
                        args.append(right)
                    elif isinstance(_val, list):
                        for arg in _val:
                            args.append(_parse(arg, parent=node))
                    elif isinstance(_val, ast.BinOp):
                        op = _parse(_val.op, _val)
                        args.append(_parse(_val.left, _val))
                        args.append(_parse(_val.right, _val))

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
                            'function': 'or',
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
                        # concatenate with or when there is more than
                        # 2 arguments in the list
                        obj = _nest(obj['args'], op, concatenator='or')
                    else:
                        obj = _nest(args, op)

            # special case when te expr is just a variable
            # make sure we parse it's content
            if not obj and isinstance(node, ast.Expression):
                return _parse(node.body)

        return obj

    if expr is None:
        return dict()

    return _parse(ast.parse(expr, mode='eval'))


def get_dataset_variables(ds):
    """
    Returns an Alias based dictionary pointing to a variable definition
    from the /api/datasets/:id/table/ endpoint

    :param ds: Dataset() instance
    :return: Dictionary keyed by alias
    """
    table = ds.follow("table", urlencode({
        'limit': 0
    }))

    # Build the variables dict, using `alias` as the key.
    variables = dict()
    for _id, var in table.metadata.items():
        var['id'] = _id
        variables[var['alias']] = var

        if var['type'] in ARRAY_TYPES:
            subreferences = var.get('subreferences', {})
            for subvar_id, subvar in subreferences.items():
                subvar['is_subvar'] = True
                subvar['id'] = subvar_id
                subvar['parent_id'] = _id
                subvar['type'] = 'categorical'
                subvar['description'] = ''
                if var.get("categories") is not None:
                    # Numeric arrays do not have categories
                    subvar['categories'] = var.get("categories")

                # TODO: This is a problem when subvariable codes are reused
                variables[subvar['alias']] = subvar
                # Poorman's square bracket lookup
                variables["%s[%s]" % (var["alias"], subvar['alias'])] = subvar

    return variables


def get_subvariables_resource(var_url, var_index):
    variable = var_index[var_url].entity
    sub_variables = variable.subvariables['index']
    return {sv['alias'].strip('#'): sv['id'] for sv in sub_variables.values()}


def _get_categories_from_var_index(vars_by_alias, var_alias):
    return vars_by_alias[var_alias].entity.body.categories


def adapt_multiple_response(var_alias, values, vars_by_alias):
    """
    Converts multiple response arguments
    to column.
    :return: the new args for multiple_response
    """
    aliases = get_subvariables_resource(var_alias, vars_by_alias)
    result = []

    if all(isinstance(value, int) for value in values):
        # scenario var.any([1])
        column = values
        variables = aliases.values()
    else:
        # scenario var.any([subvar1, subvar2])
        # in this scenario, we only want category ids that refers to `selected` categories
        column = [
            cat.get("id") for cat in _get_categories_from_var_index(vars_by_alias, var_alias) if cat.get("selected")
        ]
        subvars = [sva for sva in aliases if sva in values]

    for sva in subvars:
        result.append({
            "var": var_alias,
            'axes': [sva],
            "column": column
        })

    return result, True


def _update_values_for_multiple_response(new_values, values, subitem, vars_by_alias, arrays):
    """
    - Multiple response does not need the `value` key, but it relies on the `column` key
    - Remove from `arrays` (subvariable list) the ones that should not be considered
    """
    var_alias = subitem.get("var")
    column = new_values[0].get("column")
    value = values[0].get("value")
    if var_alias and vars_by_alias[var_alias]['type'] == 'multiple_response':
        if column:
            values[0]['column'] = column
        elif value is not None:
            values[0]['column'] = value
        values[0].pop("value", None)
        subvar_ids_by_aliases = {v['alias']: k for k, v in vars_by_alias[var_alias]['entity']['subvariables']['index'].items()}
        arrays[0] = [subvar_ids_by_aliases[new_value["axes"][0]] for new_value in new_values]


def process_expr(obj, ds):
    """
    Given a Crunch expression object (or objects) and a Dataset entity object
    (i.e. a Shoji entity), this function returns a tuple, the first element is
    new expression object (or a list of new expression objects) with all
    variable aliases transformed into variable URLs and the second element
    of the tuple is a flag indicating if the expressions needs nesting/wrapping
    in `or` functions (for the case when an array variable is passed).
    """

    base_url = ds.self
    variables = get_dataset_variables(ds)
    var_index = ds.variables.index

    def ensure_category_ids(subitems, values, arrays, variables=variables):
        var_id = None
        _subitems = []

        def variable_id(variable_url):
            return variable_url.split('/')[-2]

        def category_ids(var_alias, var_value, variables=variables):
            value = None
            if isinstance(var_value, list) or isinstance(var_value, tuple):
                # {'values': [val1, val2, ...]}
                value = []
                for val in var_value:
                    if is_number(val):
                        # val1 is an id already
                        value.append(val)
                        continue
                    for var in variables:
                        if variables[var]['alias'] == var_alias:
                            if 'categories' in variables[var]:
                                for cat in variables[var]['categories']:
                                    if cat['name'] == val:
                                        value.append(cat['id'])
                            else:
                                # variable has no categories, return original
                                # list of values
                                value = var_value
            elif isinstance(var_value, str):
                for va, var in variables.items():
                    # if the variable is a date, don't try to process it's categories
                    if var['type'] == 'datetime':
                        return var_value
                    if va == var_alias and 'categories' in var:
                        found = False
                        for cat in var['categories']:
                            if cat['name'] == var_value:
                                value = cat['id']
                                found = True
                                break
                        if not found:
                            raise ValueError("Couldn't find a category id for category %s in filter for variable %s" % (var_value, var))
                    elif 'categories' not in variables[var['alias']]:
                        return var_value

            else:
                return var_value
            return value

        # special case for multiple_response variables
        if len(subitems) == 2:
            _variable, _value = subitems
            var_alias = _variable.get('var')
            _value_key = next(iter(_value))
            if _value_key in {'column', "value"} and var_alias:
                vars_by_alias = {v['alias']: v for _, v in var_index.items()}
                if var_alias in vars_by_alias and vars_by_alias[var_alias]['type'] == 'multiple_response':
                    result = adapt_multiple_response(var_alias, _value[_value_key], vars_by_alias)
                    # handle the multiple response type
                    _update_values_for_multiple_response(result[0], values, subitems[0], vars_by_alias, arrays)
                    return result

        for item in subitems:
            if isinstance(item, dict) and 'value' in item:
                item['value'] = category_ids(var_alias, item['value'])
            _subitems.append(item)

        return _subitems, True

    def _process(obj, variables):
        op = None
        arrays = []
        values = []
        subvariables = []
        needs_wrap = True

        new_obj = copy.deepcopy(obj)
        for key, val in obj.items():
            if isinstance(val, dict) and "array" not in val:
                # This is not an array object, then it's a nested ZCL expression
                # so we need to proceed for nested processing.
                new_obj[key] = _process(val, variables)
            elif isinstance(val, (list, tuple)):
                subitems = []
                for subitem in val:
                    if isinstance(subitem, dict):
                        subitem = _process(subitem, variables)
                        if 'subvariables' in subitem:
                            arrays.append(subitem.pop('subvariables'))
                        elif 'value' in subitem or 'column' in subitem:
                            values.append(subitem)
                        elif 'var' in subitem:
                            var = variables.get(subitem['var'])
                            if var['type'] in ARRAY_TYPES and 'axes' not in subitem:
                                # Add info about the fact that the "var" referenced
                                # variable is an array, so that the validation can
                                # work properly in the remaindere of the code.
                                arrays.append(var['subvariables'])
                    subitems.append(subitem)

                has_value = any(
                    'value' in item for item in subitems if not is_number(item)
                )

                if not has_value:
                    # Since values can be see with `value` or `column` keys
                    # check if `column` is there if not `value`
                    has_value = any('column' in item for item in subitems if not is_number(item))

                has_variable = any(
                    'var' in item for item in subitems if not is_number(item)
                )

                if has_value and has_variable:
                    subitems, needs_wrap = ensure_category_ids(subitems, values, arrays)

                new_obj[key] = subitems
            elif key == 'var':
                var = variables.get(val)
                if not var:
                    raise ValueError("Invalid variable alias '%s'" % val)
                if var.get('is_subvar'):
                    parents_by_ids = {v['id']: v for v in variables.values() if not v.get('is_subvar')}
                    parent = parents_by_ids[var['parent_id']]
                    new_obj[key] = parent['alias']
                    new_obj['axes'] = [val]
            elif key == 'function':
                op = val
        
        obj = new_obj

        if subvariables:
            obj['subvariables'] = subvariables

        # support for categorical variables with `any`
        if not arrays and op == "any":
            obj["function"] = "in"

        if arrays and op in ('any', 'all', 'is_valid', 'is_missing') and needs_wrap:
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
                real_op = 'is_missing'

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

                if op == 'all':
                    inner_value = value.get("value", value.get("column", []))
                    if len(inner_value) != 1:
                        raise ValueError
                    value.pop("column", None)
                    value['value'] = inner_value[0]

                if len(subvariables) == 1:
                    obj['function'] = real_op
                    obj['args'] = [
                        {
                            'var': var['alias'],
                            'axes': [var['subreferences'][subvariables[0]]['alias']]
                        },
                        value
                    ]
                else:
                    obj = {
                        'function': expansion_op,
                        'args': []
                    }
                    args_ref = obj['args']
                    args_ref.extend(
                        [{
                            'function': real_op,
                            'args': [
                                {'var': var['alias'], 'axes': [var['subreferences'][subvar]['alias']]},
                                value
                            ]
                        } for subvar in subvariables]
                    )

        return obj

    if isinstance(obj, list):
        return [
            _process(copy.deepcopy(element), variables) for element in obj
        ]
    else:
        return _process(copy.deepcopy(obj), variables)


def clean_integer(value):
    """It cleans values that are `floats` but can be integers"""
    if isinstance(value, float) and value.is_integer():
        # Check if the value is a float and if it can be exactly represented as an integer
        value = int(value)
    return value


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
    functions = {f[1]: f[0] for f in CRUNCH_FUNC_MAP.items()}

    def _resolve_variables(_expr):
        new_expr = dict(
            function=_expr['function'],
            args=[]
        )
        for arg in _expr['args']:
            if 'function' in arg:
                # arg is a function, resolve inner variables
                new_expr['args'].append(_resolve_variables(arg))
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
            if f in ['selected', 'not_selected']:
                result = '%s(%s)%s' % (methods[f], args[0], ', '.join(str(x) for x in args[1:]))
            else:    
                result = '%s.%s(%s)' % (
                    args[0], methods[f], ', '.join(str(x) for x in args[1:])
                )
        elif f in functions:
            result = '%s(%s)' % (functions[f], args[0])
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

                if isinstance(value, float) and value.is_integer():
                    # Check if the value is a float and if it can be exactly represented as an integer
                    value = int(value)

                if isinstance(value, list):
                    value = [clean_integer(v) for v in value]

                return value
            
            if 'var' in fragment:
                if 'axes' in fragment:
                    return f"{fragment['var']}[{fragment['axes'][0]}]"
                return fragment['var']

            return list(fragment.values())[0]

        args = [_process(arg, _func) for arg in fragment['args']]
        child_functions = [
            arg.get('function')
            for arg in fragment['args'] if arg.get('function') is not None
        ]
        has_child_and_or = 'or' in child_functions
        nest = parent is not None and (
            has_child_and_or or
            (parent == 'or' and len(child_functions) > 1) or
            _func == 'or'
        )
        return _transform(_func, args, nest=nest)

    return _process(_resolve_variables(expr))
