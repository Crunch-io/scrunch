import json

import pytest
import mock
from unittest import TestCase

import scrunch
from scrunch.datasets import parse_expr
from scrunch.datasets import process_expr
from scrunch.expressions import prettify


class TestExpressionParsing(TestCase):

    def test_in_value_error(self):
        expr = "age in [{}, 1, 2]"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_invalid_method_value_error(self):
        expr = "age.invalid_method(1)"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_duplicates_value_error(self):
        expr = "age.duplicates(1)"  # duplicates doesn't accepts parameters
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_has_any_value_error(self):
        expr = "age.has_any(1,2)"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_is_valid_value_error(self):
        expr = "age.is_valid(1)"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_is_missing_value_error(self):
        expr = "age.is_missing(1)"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_is_missing_arrays(self):
        expr = "age.is_missing([1], [2])"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_unknown_function(self):
        expr = "other()"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_wrong_ops(self):
        expr = "a == 1 == 1"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_unsupported_args(self):
        expr = "valid(starargs=1)"
        with pytest.raises(ValueError) as err:
            parse_expr(expr)

    def test_expr_none(self):
        expr = None
        result = parse_expr(expr)
        assert result == {}

    def test_process_nested(self):
        expr = '(identity == 1 and caseid <= surveyid) or identity >= 2'
        vars = {
            '0001': {
                'alias': 'identity',
                'type': 'numeric'
            },
            '0002': {
                'alias': 'caseid',
                'type': 'numeric'
            },
            '0003': {
                'alias': 'surveyid',
                'type': 'numeric'
            },
        }

        obj = parse_expr(expr)
        ds = mock.MagicMock()

        def _get(*args, **kwargs):
            r = mock.MagicMock()
            r.payload.metadata = vars
            return r
        ds.session.get.side_effect = _get
        ds.self = 'http://host.com/api/datasets/abc123/'
        result = process_expr(obj, ds)
        assert result == {
            'function': 'or',
            'args': [
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': '==',
                            'args': [
                                {'variable': 'http://host.com/api/datasets/abc123/variables/0001/'},
                                {'value': 1}
                            ]
                        },
                        {
                            'function': '<=',
                            'args': [
                                {'variable': 'http://host.com/api/datasets/abc123/variables/0002/'},
                                {'variable': 'http://host.com/api/datasets/abc123/variables/0003/'}
                            ]
                        }
                    ]
                },
                {
                    'function': '>=',
                    'args': [
                        {'variable': 'http://host.com/api/datasets/abc123/variables/0001/'},
                        {'value': 2}
                    ]
                }
            ]
        }

    def test_process_invalid_variable(self):
        expr = '(identity == 1 and caseid <= surveyid) or identity >= 2'
        vars = {
            '0001': {
                'alias': 'identity',
                'type': 'numeric'
            },
            '0002': {
                'alias': 'caseid',
                'type': 'numeric'
            }
        }

        obj = parse_expr(expr)
        ds = mock.MagicMock()

        def _get(*args, **kwargs):
            r = mock.MagicMock()
            r.payload.metadata = vars
            return r
        ds.session.get.side_effect = _get
        ds.self = 'http://host.com/api/datasets/abc123/'
        with pytest.raises(ValueError) as err:
            process_expr(obj, ds)

        assert str(err.value) == "Invalid variable alias 'surveyid'"

    def test_parse_equal_int(self):
        expr = "age == 1"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'variable': 'age'
                },
                {
                    'value': 1
                }
            ]
        }

        # Reversed.
        expr = "1 == age"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'value': 1
                },
                {
                    'variable': 'age'
                }
            ]
        }

    def test_parse_equal_string(self):
        expr = "name == 'John Doe'"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'variable': 'name'
                },
                {
                    'value': 'John Doe'
                }
            ]
        }

        # Reversed.
        expr = "'John Doe' == name"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'value': 'John Doe'
                },
                {
                    'variable': 'name'
                }
            ]
        }

    def test_parse_notequal_int(self):
        expr = "age != 1"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '!=',
            'args': [
                {
                    'variable': 'age'
                },
                {
                    'value': 1
                }
            ]
        }

        # Reversed.
        expr = "1 != age"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '!=',
            'args': [
                {
                    'value': 1
                },
                {
                    'variable': 'age'
                }
            ]
        }

    def test_parse_notequal_string(self):
        expr = "name != 'John Doe'"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '!=',
            'args': [
                {
                    'variable': 'name'
                },
                {
                    'value': 'John Doe'
                }
            ]
        }

        # Reversed.
        expr = "'John Doe' != name"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '!=',
            'args': [
                {
                    'value': 'John Doe'
                },
                {
                    'variable': 'name'
                }
            ]
        }

    def test_parse_less_than(self):
        expr = "caseid < 1234"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '<',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }

        # Reversed.
        expr = "1234 < caseid"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '<',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }

    def test_parse_less_than_equal(self):
        expr = "caseid <= 1234"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '<=',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }

        # Reversed.
        expr = "1234 <= caseid"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '<=',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }

    def test_parse_greater_than(self):
        expr = "caseid > 1234"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '>',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }

        # Reversed.
        expr = "1234 > caseid"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '>',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }

    def test_parse_greater_than_equal(self):
        expr = "caseid >= 1234"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '>=',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }

        # Reversed.
        expr = "1234 >= caseid"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '>=',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }

    def test_parse_compare_variable_against_another_variable(self):
        expr = "starttdate == arrivedate"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }

        expr = "starttdate != arrivedate"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '!=',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }

        expr = "starttdate < arrivedate"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '<',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }

        expr = "starttdate <= arrivedate"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '<=',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }

        expr = "starttdate > arrivedate"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '>',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }

        expr = "starttdate >= arrivedate"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': '>=',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }

    def test_parse_multiple_boolean_conditions(self):
        expr = '(identity == 1 and caseid <= surveyid) or identity >= 2'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'or',
            'args': [
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': '==',
                            'args': [
                                {
                                    'variable': 'identity'
                                },
                                {
                                    'value': 1
                                }
                            ]
                        },
                        {
                            'function': '<=',
                            'args': [
                                {
                                    'variable': 'caseid'
                                },
                                {
                                    'variable': 'surveyid'
                                }
                            ]
                        }
                    ]
                },
                {
                    'function': '>=',
                    'args': [
                        {
                            'variable': 'identity'
                        },
                        {
                            'value': 2
                        }
                    ]
                }
            ]
        }

    def test_parse_value_in_list(self):
        expr = "web_browser in ['abc', 'dfg', 'hij']"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': 'web_browser'
                },
                {
                    'value': ['abc', 'dfg', 'hij']
                }
            ]
        }

        # Tuples should also be supported.
        expr = "web_browser in ('abc', 'dfg', 'hij')"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': 'web_browser'
                },
                {
                    'value': ['abc', 'dfg', 'hij']
                }
            ]
        }

    def test_parse_value_not_in_list(self):
        expr = 'country not in [1, 2, 3]'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'country'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }

        # Tuples should also be supported.
        expr = 'country not in (1, 2, 3)'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'country'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }

    def test_parse_sample_rule_1(self):
        # 'text': 'diposition code 0 (screenouts)',
        # 'index_mapper': intersection(
        #        [{'disposition': [0]}, {'exit_status': [0]}])})
        expr = "disposition == 0 and exit_status == 0"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'and',
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

    def test_parse_has_any(self):
        expr = 'Q2.has_any([1, 2, 3])'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'any',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': [1, 2, 3]
                }
            ]
        }

        expr = 'Q2.has_any((1, 2, 3))'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'any',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': [1, 2, 3]
                }
            ]
        }

        expr = 'Q2.has_any(1)'
        with pytest.raises(ValueError):
            parse_expr(expr)

        expr = 'Q2.has_any(Q3)'
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_parse_has_all(self):
        expr = 'Q2.has_all([1, 2, 3])'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'all',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': [1, 2, 3]
                }
            ]
        }

        expr = 'Q2.has_all((1, 2, 3))'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'all',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': [1, 2, 3]
                }
            ]
        }

        expr = 'Q2.has_all(1)'
        with pytest.raises(ValueError):
            parse_expr(expr)

        expr = 'Q2.has_all(Q3)'
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_parse_has_count(self):
        expr = 'Q2.has_count(1)'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'has_count',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': 1
                }
            ]
        }

        expr = 'Q2.has_count(1, 2)'
        with pytest.raises(ValueError):
            parse_expr(expr)

        expr = 'Q2.has_count([1,2])'
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_parse_sample_rule_2_complex(self):
        # Lets combine this with the previous one:
        # 'text': 'diposition code 0 (quotafull)',
        # 'index_mapper': intersection(
        #     [{'disposition': [0]}, {'exit_status': [1]}])
        expr = "(disposition == 0 and exit_status == 1) or " \
               "(disposition == 0 and exit_status == 0)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'or',
            'args': [{
                    'function': 'and',
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
                                    'value': 1
                                }
                            ]
                        }
                    ]
                },
                {
                    'function': 'and',
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
            ]}

    def test_parse_sample_has_any(self):
        # 'text': 'CompanyTurnover is NA',
        # 'index_mapper': {'CompanyTurnover': has_any([99])}},

        # 'text': 'Not Private Sector',
        # 'index_mapper': {'sector': has_any([2, 3, 98, 99])}},
        expr = "CompanyTurnover.has_any([99])"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'any',
            'args': [
                {
                    'variable': 'CompanyTurnover'
                },
                {
                    'value': [99]
                }
            ]
        }

        expr = "sector.has_any([2, 3, 98, 99])"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'any',
            'args': [
                {
                    'variable': 'sector'
                },
                {
                    'value': [2, 3, 98, 99]
                }
            ]
        }

    def test_parse_negated_expr(self):
        expr = "not (age == 1)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': '==',
                    'args': [
                        {
                            'variable': 'age'
                        },
                        {
                            'value': 1
                        }
                    ]
                }
            ]
        }

    def test_parse_negated_method_call(self):
        expr = 'not Q2.has_any([1, 2, 3])'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'any',
                    'args': [
                        {
                            'variable': 'Q2'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }

        expr = 'not Q2.has_all([1, 2, 3])'
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'all',
                    'args': [
                        {
                            'variable': 'Q2'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }

    def test_parse_duplicates_method(self):
        expr = "identity.duplicates()"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'duplicates',
            'args': [
                {
                    'variable': 'identity'
                }
            ]
        }

        # Negated.
        expr = "not identity.duplicates()"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'duplicates',
                    'args': [
                        {
                            'variable': 'identity'
                        }
                    ]
                }
            ]
        }

        # Parameters not allowed.
        with pytest.raises(ValueError):
            parse_expr("identity.duplicates([1,2,3])")

        with pytest.raises(ValueError):
            parse_expr("identity.duplicates(1)")

        with pytest.raises(ValueError):
            parse_expr("identity.duplicates('hello')")

        with pytest.raises(ValueError):
            parse_expr("identity.duplicates(False)")

    def test_multiple_and_or(self):
        expr = 'age == 1 and test == 3 and myop == "age"'
        expected = {
            'args': [
                {
                    'args': [
                        {'variable': 'age'},
                        {'value': 1}
                    ],
                    'function': '=='
                },
                {
                    'args': [
                        {
                            'args': [
                                {'variable': 'test'},
                                {'value': 3}
                            ],
                            'function': '=='
                        },
                        {
                            'args': [
                                {'variable': 'myop'},
                                {'value': 'age'}
                            ],
                            'function': '=='
                        }
                    ],
                   'function': 'and'
                }
            ],
            'function': 'and'
        }
        expr_obj = parse_expr(expr)
        assert expr_obj == expected

    def test_arithmetic_operations(self):
        expr = "var1 + 3 == var2 - 2 and var3 / 1 == var4 * 10"
        expected = {
            'args': [
                {
                    'args': [
                        {
                            'args': [
                                {'variable': 'var1'},
                                {'value': 3}
                            ],
                            'function': '+'
                        },
                        {
                            'args': [
                                {'variable': 'var2'},
                                {'value': 2}
                            ],
                            'function': '-'}
                    ],
                    'function': '=='
                },
                {
                    'args': [
                        {
                            'args': [
                                {'variable': 'var3'},
                                {'value': 1}
                            ],
                            'function': '/'
                        },
                        {
                            'args': [
                                {'variable': 'var4'},
                                {'value': 10}
                            ],
                            'function': '*'}
                    ],
                    'function': '=='
                }
            ],
            'function': 'and'
        }
        expr_obj = parse_expr(expr)
        assert expr_obj == expected

    def test_arithmetic_operator_presedence(self):
        expr = "var1 * 10 + 3 / 2 == var2"
        expected = {
            'args': [
                {
                    'args': [
                        {
                            'args': [
                                {'variable': 'var1'},
                                {'value': 10}
                            ],
                            'function': '*'
                        },
                        {
                            'args': [
                                {'value': 3},
                                {'value': 2}
                            ],
                            'function': '/'
                        }
                    ],
                    'function': '+'
                },
                {'variable': 'var2'}
            ],
            'function': '=='
        }
        expr_obj = parse_expr(expr)
        assert expr_obj == expected

    def test_parse_helper_functions(self):
        # One variable.
        expr = "valid(birthyear)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'is_valid',
            'args': [
                {
                    'variable': 'birthyear'
                }
            ]
        }

        expr = "missing(birthyear)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'is_missing',
            'args': [
                {
                    'variable': 'birthyear'
                }
            ]
        }

        # One variable, negated.
        expr = "not valid(birthyear)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'is_valid',
                    'args': [
                        {
                            'variable': 'birthyear'
                        }
                    ]
                }
            ]
        }

        expr = "not missing(birthyear)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'is_missing',
                    'args': [
                        {
                            'variable': 'birthyear'
                        }
                    ]
                }
            ]
        }

        # Multiple variables.
        expr = "valid(birthyear, birthmonth)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'and',
            'args': [
                {
                    'function': 'is_valid',
                    'args': [
                        {
                            'variable': 'birthyear'
                        }
                    ]
                },
                {
                    'function': 'is_valid',
                    'args': [
                        {
                            'variable': 'birthmonth'
                        }
                    ]
                }
            ]
        }

        expr = "missing(birthyear, birthmonth)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'and',
            'args': [
                {
                    'function': 'is_missing',
                    'args': [
                        {
                            'variable': 'birthyear'
                        }
                    ]
                },
                {
                    'function': 'is_missing',
                    'args': [
                        {
                            'variable': 'birthmonth'
                        }
                    ]
                }
            ]
        }

        # Multiple variables, negated.
        expr = "not valid(birthyear, birthmonth)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': 'is_valid',
                            'args': [
                                {
                                    'variable': 'birthyear'
                                }
                            ]
                        },
                        {
                            'function': 'is_valid',
                            'args': [
                                {
                                    'variable': 'birthmonth'
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        expr = "not missing(birthyear, birthmonth)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': 'is_missing',
                            'args': [
                                {
                                    'variable': 'birthyear'
                                }
                            ]
                        },
                        {
                            'function': 'is_missing',
                            'args': [
                                {
                                    'variable': 'birthmonth'
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # More advanced combinations.
        expr = "caseid < 12345 and missing(birthyear, birthmonth)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'and',
            'args': [
                {
                    'function': '<',
                    'args': [
                        {
                            'variable': 'caseid'
                        },
                        {
                            'value': 12345
                        }
                    ]
                },
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': 'is_missing',
                            'args': [
                                {
                                    'variable': 'birthyear'
                                }
                            ]
                        },
                        {
                            'function': 'is_missing',
                            'args': [
                                {
                                    'variable': 'birthmonth'
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def test_parse_not_a_in_b(self):
        expr = "a not in [1, 2, 3]"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'a'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }

        expr = "not a in [1, 2, 3]"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'a'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }

        expr = "not (a in [1, 2, 3])"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'a'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }

# 'diposition code 0 (incompletes)':
# intersection(
#     [{'disposition': not_any([1])},
#      union([{'exit_status': has_count(0)},
#             {'exit_status': is_ge(1)}])
#      ]
# )




# 'text': 'sta: nicht aus Deutschland',
# 'index_mapper': {'sta': has_any([17])}},

# 'text': '(age >= 18) & profile_julesage is NaN',
#             'index_mapper': intersection(
#                 [
#                     {'age': is_ge(18)},
#                     {'profile_julesage': has_count(0)}])},

#'text': '(age >= 18) & profile_bpcagesex is NaN',
# 'index_mapper': intersection(
#     [{'age': is_ge(18)}, {'profile_bpcagesex': has_count(0)}])}],

# 'text': 'LONDON 18 NAN (profile_bpcagesex)',
# 'index_mapper': intersection(
#     [{'age': is_ge(18)}, {'profile_bpcagesex': has_count(0)}])},

# 'text': 'profile_GOR not code 11',
# 'index_mapper': {'profile_GOR': not_any([11])}}],

# 'text': '(age >= 18) & profile_julesage is NaN',
# 'index_mapper': intersection(
#     [{'age': is_ge(18)}, {'profile_julesage': has_count(0)}])},


# 'text': 'Not the right decision maker',
# 'index_mapper': {'DecisionMaking2': not_any(frange('1-10'))}},

# 'text': 'Duplicate identity',
# 'columns': 'identity',
# 'duplicated': True}])


# {  Drop anything missing (not asked/skipped/don't know/missing)
#     'text': 'DE PET OWNER NaN',
#     'columns': [
#                 'age_sample_18', 'gender',
#                 'nielsenregion', ''],
#     'dropna': True}


# { 'text': 'pets_sample not codes 1-4',
#     'index_mapper': {'pets_sample': not_any([1, 2, 3, 4])}}]


class TestExpressionProcessing(TestCase):

    ds_url = 'http://test.crunch.io/api/datasets/123/'

    class CrunchPayload(dict):
        def __getattr__(self, item):
            if item == 'payload':
                return self
            else:
                return self[item]

    @staticmethod
    def _build_get_func(**kwargs):
        props = {}
        props.update(kwargs)

        def _get(*args):
            return props.get(args[0], args[0])

        return _get

    def test_transform_alias_to_var_id(self):
        var_id = '0001'
        var_alias = 'age'
        var_type = 'numeric'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)

        # Mock the dataset.
        _get_func = self._build_get_func(
            id=var_id, type=var_type, alias=var_alias, is_subvar=False
        )
        _var_mock = mock.MagicMock()
        _var_mock.entity.self = var_url
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return self.CrunchPayload({
                    'metadata': {
                        var_id: _var_mock
                    }
                })
            return self.CrunchPayload()

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.session.get.side_effect = _session_get

        expr_obj = process_expr(parse_expr('age == 1'), ds)

        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'variable': var_url
                },
                {
                    'value': 1
                }
            ]
        }

    def test_transform_subvar_alias_to_subvar_id(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001',
            '0002'
        ]
        subreferences = [
            {
                'alias': 'hobbies_1'
            },
            {
                'alias': 'hobbies_2'
            }
        ]

        # Mock the dataset.
        _get_func = self._build_get_func(
            id=var_id, type=var_type, alias=var_alias, is_subvar=False,
            subvariables=subvariables,
            subreferences=subreferences
        )
        _var_mock = mock.MagicMock()
        _var_mock.entity.self = var_url
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func
        _var_mock.subvariables = subvariables
        _var_mock.subreferences = subreferences

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return self.CrunchPayload({
                    'metadata': {
                        var_id: _var_mock
                    }
                })
            return self.CrunchPayload()

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.session.get.side_effect = _session_get

        expr = 'hobbies_1 == 4'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                },
                {
                    'value': 4
                }
            ]
        }

    def test_has_any_processing(self):
        expr = 'a.has_any(["1", "2"])'
        parsed = parse_expr(expr)
        assert parsed == {
            'function': 'any',
            'args': [{'variable': 'a'}, {'value': ['1', '2']}],
        }
        urls_map = {
            'a': {'id': 'url/a', 'type': 'multiple_response', 'subvariables': ['1', '2']},
            '1': {'id': 'url/1', 'type': 'categorical', 'is_subvar': True},
            '2': {'id': 'url/2', 'type': 'categorical', 'is_subvar': True},
        }
        ds = mock.MagicMock()
        ds.self = '/dataset/'
        gdv = lambda ds: urls_map
        with mock.patch('scrunch.expressions.get_dataset_variables', gdv):
            processed = process_expr(parsed, ds)

        assert processed == {
            'function': 'any',
            'args': [
                {
                    'variable': '/dataset/variables/url/a/'
                },
                {
                    'value': [
                        '/dataset/variables/url/a/subvariables/1/',
                        '/dataset/variables/url/a/subvariables/2/'
                    ]
                }
            ]
        }

    def test_array_expansion_single_subvariable(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001'
        ]
        subreferences = [
            {
                'alias': 'hobbies_1'
            }
        ]

        # Mock the dataset.
        _get_func = self._build_get_func(
            id=var_id, type=var_type, alias=var_alias, is_subvar=False,
            subvariables=subvariables,
            subreferences=subreferences
        )
        _var_mock = mock.MagicMock()
        _var_mock.entity.self = var_url
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func
        _var_mock.subvariables = subvariables
        _var_mock.subreferences = subreferences

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return self.CrunchPayload({
                    'metadata': {
                        var_id: _var_mock
                    }
                })
            return self.CrunchPayload()

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.session.get.side_effect = _session_get

        # Single value.
        expr_obj = process_expr(parse_expr('hobbies.has_any([32766])'), ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                },
                {
                    'value': [32766]
                }
            ]
        }

        expr_obj = process_expr(parse_expr('hobbies.has_all([32766])'), ds)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                },
                {
                    'value': 32766
                }
            ]
        }

        # Negated.
        expr_obj = process_expr(parse_expr('not hobbies.has_any([32766])'), ds)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                        },
                        {
                            'value': [32766]
                        }
                    ]
                }

            ]
        }

        expr_obj = process_expr(parse_expr('not hobbies.has_all([32766])'), ds)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': '==',
                    'args': [
                        {
                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                        },
                        {
                            'value': 32766
                        }
                    ]
                }

            ]
        }

        # Multiple values.
        expr_obj = process_expr(parse_expr('hobbies.has_any([32766, 32767])'), ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                },
                {
                    'value': [32766, 32767]
                }
            ]
        }

        with pytest.raises(ValueError):
            process_expr(parse_expr('hobbies.has_all([32766, 32767])'), ds)

    def test_array_expansion_multiple_subvariables(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001',
            '0002',
            '0003',
            '0004'
        ]
        subreferences = [
            {
                'alias': 'hobbies_1'
            },
            {
                'alias': 'hobbies_2'
            },
            {
                'alias': 'hobbies_3'
            },
            {
                'alias': 'hobbies_4'
            }
        ]

        # Mock the dataset.
        _get_func = self._build_get_func(
            id=var_id, type=var_type, alias=var_alias, is_subvar=False,
            subvariables=subvariables,
            subreferences=subreferences
        )
        _var_mock = mock.MagicMock()
        _var_mock.entity.self = var_url
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func
        _var_mock.subvariables = subvariables
        _var_mock.subreferences = subreferences

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return self.CrunchPayload({
                    'metadata': {
                        var_id: _var_mock
                    }
                })
            return self.CrunchPayload()

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.session.get.side_effect = _session_get

        # Single values.
        expr = 'hobbies.has_any([32766])'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'or',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                        },
                        {
                            'value': [32766]
                        }
                    ]
                },
                {
                    'function': 'or',
                    'args': [
                        {
                            'function': 'in',
                            'args': [
                                {
                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[1])
                                },
                                {
                                    'value': [32766]
                                }
                            ]
                        },
                        {
                            'function': 'or',
                            'args': [
                                {
                                    'function': 'in',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[2])
                                        },
                                        {
                                            'value': [32766]
                                        }
                                    ]
                                },
                                {
                                    'function': 'in',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[3])
                                        },
                                        {
                                            'value': [32766]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        expr = 'hobbies.has_all([32766])'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'and',
            'args': [
                {
                    'function': '==',
                    'args': [
                        {
                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                        },
                        {
                            'value': 32766
                        }
                    ]
                },
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': '==',
                            'args': [
                                {
                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[1])
                                },
                                {
                                    'value': 32766
                                }
                            ]
                        },
                        {
                            'function': 'and',
                            'args': [
                                {
                                    'function': '==',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[2])
                                        },
                                        {
                                            'value': 32766
                                        }
                                    ]
                                },
                                {
                                    'function': '==',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[3])
                                        },
                                        {
                                            'value': 32766
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Negated.
        expr = 'not hobbies.has_any([32766])'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'or',
                    'args': [
                        {
                            'function': 'in',
                            'args': [
                                {
                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                                },
                                {
                                    'value': [32766]
                                }
                            ]
                        },
                        {
                            'function': 'or',
                            'args': [
                                {
                                    'function': 'in',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[1])
                                        },
                                        {
                                            'value': [32766]
                                        }
                                    ]
                                },
                                {
                                    'function': 'or',
                                    'args': [
                                        {
                                            'function': 'in',
                                            'args': [
                                                {
                                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[2])
                                                },
                                                {
                                                    'value': [32766]
                                                }
                                            ]
                                        },
                                        {
                                            'function': 'in',
                                            'args': [
                                                {
                                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[3])
                                                },
                                                {
                                                    'value': [32766]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        expr = 'not hobbies.has_all([32766])'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': '==',
                            'args': [
                                {
                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                                },
                                {
                                    'value': 32766
                                }
                            ]
                        },
                        {
                            'function': 'and',
                            'args': [
                                {
                                    'function': '==',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[1])
                                        },
                                        {
                                            'value': 32766
                                        }
                                    ]
                                },
                                {
                                    'function': 'and',
                                    'args': [
                                        {
                                            'function': '==',
                                            'args': [
                                                {
                                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[2])
                                                },
                                                {
                                                    'value': 32766
                                                }
                                            ]
                                        },
                                        {
                                            'function': '==',
                                            'args': [
                                                {
                                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[3])
                                                },
                                                {
                                                    'value': 32766
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Multiple values.
        expr = 'hobbies.has_any([32766, 32767])'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'or',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                        },
                        {
                            'value': [32766, 32767]
                        }
                    ]
                },
                {
                    'function': 'or',
                    'args': [
                        {
                            'function': 'in',
                            'args': [
                                {
                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[1])
                                },
                                {
                                    'value': [32766, 32767]
                                }
                            ]
                        },
                        {
                            'function': 'or',
                            'args': [
                                {
                                    'function': 'in',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[2])
                                        },
                                        {
                                            'value': [32766, 32767]
                                        }
                                    ]
                                },
                                {
                                    'function': 'in',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[3])
                                        },
                                        {
                                            'value': [32766, 32767]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Multiple values, negated
        expr = 'not hobbies.has_any([32766, 32767])'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'or',
                    'args': [
                        {
                            'function': 'in',
                            'args': [
                                {
                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[0])
                                },
                                {
                                    'value': [32766, 32767]
                                }
                            ]
                        },
                        {
                            'function': 'or',
                            'args': [
                                {
                                    'function': 'in',
                                    'args': [
                                        {
                                            'variable': '%ssubvariables/%s/' % (var_url, subvariables[1])
                                        },
                                        {
                                            'value': [32766, 32767]
                                        }
                                    ]
                                },
                                {
                                    'function': 'or',
                                    'args': [
                                        {
                                            'function': 'in',
                                            'args': [
                                                {
                                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[2])
                                                },
                                                {
                                                    'value': [32766, 32767]
                                                }
                                            ]
                                        },
                                        {
                                            'function': 'in',
                                            'args': [
                                                {
                                                    'variable': '%ssubvariables/%s/' % (var_url, subvariables[3])
                                                },
                                                {
                                                    'value': [32766, 32767]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def test_valid_and_missing_funcs_for_arrays(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001',
            '0002',
            '0003',
            '0004'
        ]
        subreferences = [
            {
                'alias': 'hobbies_1'
            },
            {
                'alias': 'hobbies_2'
            },
            {
                'alias': 'hobbies_3'
            },
            {
                'alias': 'hobbies_4'
            }
        ]

        # Mock the dataset.
        _get_func = self._build_get_func(
            id=var_id, type=var_type, alias=var_alias, is_subvar=False,
            subvariables=subvariables,
            subreferences=subreferences
        )
        _var_mock = mock.MagicMock()
        _var_mock.entity.self = var_url
        _var_mock.__getitem__.side_effect = _get_func
        _var_mock.get.side_effect = _get_func
        _var_mock.subvariables = subvariables
        _var_mock.subreferences = subreferences

        def _session_get(*args, **kwargs):
            if args[0] == '%stable/' % self.ds_url:
                return self.CrunchPayload({
                    'metadata': {
                        var_id: _var_mock
                    }
                })
            return self.CrunchPayload()

        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.fragments.table = '%stable/' % self.ds_url
        ds.session.get.side_effect = _session_get

        expr = 'valid(hobbies)'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'all_valid',
            'args': [
                {
                    'variable': var_url
                }
            ]
        }

        expr = 'not valid(hobbies)'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'all_valid',
                    'args': [
                        {
                            'variable': var_url
                        }
                    ]
                }
            ]
        }

        expr = 'missing(hobbies)'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'all_missing',
            'args': [
                {
                    'variable': var_url
                }
            ]
        }

        expr = 'not missing(hobbies)'
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'not',
            'args': [
                {
                    'function': 'all_missing',
                    'args': [
                        {
                            'variable': var_url
                        }
                    ]
                }
            ]
        }


class TestExpressionPrettify(TestCase):

    def test_simple_eq(self):
        expr = {
            'function': '==',
            'args': [
                {
                    'variable': 'age'
                },
                {
                    'value': 1
                }
            ]
        }

        expected = 'age == 1'
        cel = prettify(expr)
        assert expected == cel

    def test_and(self):
        expr = {
            'function': 'and',
            'args': [
                {
                    'function': '>',
                    'args': [
                        {
                            'variable': 'age'
                        },
                        {
                            'value': 1
                        }
                    ]
                },
                {
                    'function': '==',
                    'args': [
                        {
                            'variable': 'favcolor'
                        },
                        {
                            'value': 2
                        }
                    ]
                }
            ]
        }

        expected = 'age > 1 and favcolor == 2'
        cel = prettify(expr)
        assert expected == cel

    def test_nested_or(self):
        expr = {
            'function': 'and',
            'args': [
                {
                    'function': '>',
                    'args': [
                        {
                            'variable': 'age'
                        },
                        {
                            'value': 1
                        }
                    ]
                },
                {
                    'function': 'or',
                    'args': [

                        {
                            'function': '==',
                            'args': [
                                {
                                    'variable': 'favcolor'
                                },
                                {
                                    'value': 2
                                }
                            ]
                        },
                        {
                            'function': '==',
                            'args': [
                                {
                                    'variable': 'genre'
                                },
                                {
                                    'value': 1
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        expected = 'age > 1 and (favcolor == 2 or genre == 1)'
        cel = prettify(expr)
        assert expected == cel

    def test_complex(self):
        expr = {
            'function': 'and',
            'args': [
                {
                    'function': '>',
                    'args': [
                        {
                            'variable': 'age'
                        },
                        {
                            'value': 55
                        }
                    ]
                },
                {
                    'function': 'or',
                    'args': [
                        {
                            'function': 'and',
                            'args': [
                                {
                                    'function': '==',
                                    'args': [
                                        {
                                            'variable': 'genre'
                                        },
                                        {
                                            'value': 1
                                        }
                                    ]
                                },
                                {
                                    'function': '==',
                                    'args': [
                                        {
                                            'variable': 'favfruit'
                                        },
                                        {
                                            'value': 9
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            'function': 'in',
                            'args': [
                                {
                                    'variable': 'favcolor'
                                },
                                {
                                    'value': [3, 4, 5]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        expected = 'age > 55 and ((genre == 1 and favfruit == 9) or favcolor in [3, 4, 5])'
        cel = prettify(expr)
        assert expected == cel

    def test_variable_url(self):
        expr = {
            'function': '==',
            'args': [
                {
                    'variable': 'https://host.com/api/datasets/123/variables/001/'
                },
                {
                    'value': 1
                }
            ]
        }

        ds = mock.MagicMock()
        ds.__class__ = scrunch.datasets.Dataset
        response = mock.MagicMock()
        response.payload.body.alias = 'age'

        ds.session.get.side_effect = lambda *arg: response

        expected = 'age == 1'
        cel = prettify(expr, ds)
        assert expected == cel
        ds.session.get.assert_called_with('https://host.com/api/datasets/123/variables/001/')

    def test_variable_url_no_dataset(self):
        expr = {
            'function': '==',
            'args': [
                {
                    'variable': 'https://host.com/api/datasets/123/variables/001/'
                },
                {
                    'value': 1
                }
            ]
        }

        with pytest.raises(Exception) as err:
            prettify(expr)

        assert str(err.value) == (
            'Valid Dataset instance is required to resolve variable urls '
            'in the expression'
        )

    def test_parse_equal_string(self):
        expr_obj = {
            'function': '==',
            'args': [
                {
                    'variable': 'name'
                },
                {
                    'value': 'John Doe'
                }
            ]
        }
        cel = prettify(expr_obj)
        assert cel == "name == 'John Doe'"

        # Reversed.
        expr_obj = {
            'function': '==',
            'args': [
                {
                    'value': 'John Doe'
                },
                {
                    'variable': 'address'
                }
            ]
        }
        cel = prettify(expr_obj)
        assert cel == "'John Doe' == address"

    def test_parse_equal_string_escape_quote(self):
        expr_obj = {
            'function': '==',
            'args': [
                {
                    'value': '''John's Name'''
                },
                {
                    'variable': 'address'
                }
            ]
        }
        cel = prettify(expr_obj)
        # Actually is a single backslash escaping the quote,
        # but we need to escape the actual backslash and quote
        # for this comparisson
        assert cel == "'John\\\'s Name' == address"

    def test_parse_notequal_int(self):
        expr = {
            'function': '!=',
            'args': [
                {
                    'variable': 'age'
                },
                {
                    'value': 1
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "age != 1"

        # Reversed.
        expr = {
            'function': '!=',
            'args': [
                {
                    'value': 1
                },
                {
                    'variable': 'age'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "1 != age"

    def test_parse_notequal_string(self):
        expr = {
            'function': '!=',
            'args': [
                {
                    'variable': 'name'
                },
                {
                    'value': 'John Doe'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "name != 'John Doe'"

        # Reversed.
        expr = {
            'function': '!=',
            'args': [
                {
                    'value': 'John Doe'
                },
                {
                    'variable': 'name'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "'John Doe' != name"

    def test_parse_less_than(self):
        expr = {
            'function': '<',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "caseid < 1234"

        # Reversed.
        expr = {
            'function': '<',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "1234 < caseid"

    def test_parse_less_than_equal(self):
        expr = {
            'function': '<=',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "caseid <= 1234"

        # Reversed.
        expr = {
            'function': '<=',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "1234 <= caseid"

    def test_parse_greater_than(self):
        expr = {
            'function': '>',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "caseid > 1234"

        # Reversed.
        expr = {
            'function': '>',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "1234 > caseid"

    def test_parse_greater_than_equal(self):
        expr = {
            'function': '>=',
            'args': [
                {
                    'variable': 'caseid'
                },
                {
                    'value': 1234
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "caseid >= 1234"

        # Reversed.
        expr = {
            'function': '>=',
            'args': [
                {
                    'value': 1234
                },
                {
                    'variable': 'caseid'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "1234 >= caseid"

    def test_parse_compare_variable_against_another_variable(self):
        expr = {
            'function': '==',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "starttdate == arrivedate"

        expr = {
            'function': '!=',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "starttdate != arrivedate"

        expr = {
            'function': '<',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "starttdate < arrivedate"

        expr = {
            'function': '<=',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "starttdate <= arrivedate"

        expr = {
            'function': '>',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "starttdate > arrivedate"

        expr = {
            'function': '>=',
            'args': [
                {
                    'variable': 'starttdate'
                },
                {
                    'variable': 'arrivedate'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "starttdate >= arrivedate"

    def test_parse_multiple_boolean_conditions(self):
        expr = {
            'function': 'or',
            'args': [
                {
                    'function': 'and',
                    'args': [
                        {
                            'function': '==',
                            'args': [
                                {
                                    'variable': 'identity'
                                },
                                {
                                    'value': 1
                                }
                            ]
                        },
                        {
                            'function': '<=',
                            'args': [
                                {
                                    'variable': 'caseid'
                                },
                                {
                                    'variable': 'surveyid'
                                }
                            ]
                        }
                    ]
                },
                {
                    'function': '>=',
                    'args': [
                        {
                            'variable': 'identity'
                        },
                        {
                            'value': 2
                        }
                    ]
                }
            ]
        }
        cel = prettify(expr)
        assert cel =='(identity == 1 and caseid <= surveyid) or identity >= 2'

    def test_parse_value_in_list(self):
        expr = {
            'function': 'in',
            'args': [
                {
                    'variable': 'web_browser'
                },
                {
                    'value': ['abc', 'dfg', 'hij']
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "web_browser in ['abc', 'dfg', 'hij']"

    def test_parse_value_not_in_list(self):
        expr = {
            'function': 'not',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'country'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }
        cel = prettify(expr)

        # TODO: look for improvements:
        #   despite it is valid, seems better to have
        #   `x not in y` than `not x in y`
        # assert cel == 'country not in [1, 2, 3]'
        assert cel == 'not country in [1, 2, 3]'

    def test_parse_sample_rule_1(self):

        expr = {
            'function': 'and',
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
        cel = prettify(expr)
        assert cel == "disposition == 0 and exit_status == 0"

    def test_parse_has_any(self):
        expr = {
            'function': 'any',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': [1, 2, 3]
                }
            ]
        }
        cel = prettify(expr)
        assert cel =='Q2.has_any([1, 2, 3])'

    def test_parse_has_all(self):
        expr = {
            'function': 'all',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': [1, 2, 3]
                }
            ]
        }
        cel = prettify(expr)
        assert cel =='Q2.has_all([1, 2, 3])'

    def test_parse_has_count(self):
        expr = {
            'function': 'has_count',
            'args': [
                {
                    'variable': 'Q2'
                },
                {
                    'value': 1
                }
            ]
        }
        cel = prettify(expr)
        assert cel =='Q2.has_count(1)'

    def test_parse_sample_rule_2_complex(self):
        expr = {
            'function': 'or',
            'args': [{
                    'function': 'and',
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
                                    'value': 1
                                }
                            ]
                        }
                    ]
                },
                {
                    'function': 'and',
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
            ]}
        cel = prettify(expr)
        assert cel == "(disposition == 0 and exit_status == 1) or " \
               "(disposition == 0 and exit_status == 0)"

    def test_parse_sample_has_any(self):
        expr = {
            'function': 'any',
            'args': [
                {
                    'variable': 'CompanyTurnover'
                },
                {
                    'value': [99]
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "CompanyTurnover.has_any([99])"

        expr = {
            'function': 'any',
            'args': [
                {
                    'variable': 'sector'
                },
                {
                    'value': [2, 3, 98, 99]
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "sector.has_any([2, 3, 98, 99])"

    def test_parse_negated_expr(self):
        expr = {
            'function': 'not',
            'args': [
                {
                    'function': '==',
                    'args': [
                        {
                            'variable': 'age'
                        },
                        {
                            'value': 1
                        }
                    ]
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "not age == 1"

    def test_parse_negated_method_call(self):
        expr = {
            'function': 'not',
            'args': [
                {
                    'function': 'any',
                    'args': [
                        {
                            'variable': 'Q2'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }
        cel = prettify(expr)
        assert cel =='not Q2.has_any([1, 2, 3])'

        expr = {
            'function': 'not',
            'args': [
                {
                    'function': 'all',
                    'args': [
                        {
                            'variable': 'Q2'
                        },
                        {
                            'value': [1, 2, 3]
                        }
                    ]
                }
            ]
        }
        cel = prettify(expr)
        assert cel =='not Q2.has_all([1, 2, 3])'

    def test_parse_duplicates_method(self):
        expr = {
            'function': 'duplicates',
            'args': [
                {
                    'variable': 'identity'
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "identity.duplicates()"

        # Negated.
        expr = {
            'function': 'not',
            'args': [
                {
                    'function': 'duplicates',
                    'args': [
                        {
                            'variable': 'identity'
                        }
                    ]
                }
            ]
        }
        cel = prettify(expr)
        assert cel == "not identity.duplicates()"

    def test_unknown_function(self):
        expr = {
            'function': '>>',  # Assuming this is a typo
            'args': [
                {
                    'variable': 'identity'
                },
                {
                    'value': 1
                }
            ]
        }
        with pytest.raises(Exception) as err:
            prettify(expr)

        assert str(err.value) == 'Unknown function ">>"'

