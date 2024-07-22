import sys

import pytest
import mock
from unittest import TestCase

import scrunch
from scrunch.datasets import parse_expr
from scrunch.datasets import process_expr
from scrunch.expressions import prettify, adapt_multiple_response, get_dataset_variables
from scrunch.tests.conftest import mark_fail_py2


class TestExpressionParsing(TestCase):

    def test_any_of_str(self):
        expr = '"age".any(1,2)'

        with pytest.raises(SyntaxError):
            parse_expr(expr)

    def test_in_value_error(self):
        expr = "age in [{}, 1, 2]"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_invalid_method_value_error(self):
        expr = "age.invalid_method(1)"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_duplicates_value_error(self):
        expr = "age.duplicates(1)"  # duplicates doesn't accepts parameters
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_any_value_error(self):
        expr = "age.any(1,2)"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_is_valid_value_error(self):
        expr = "age.is_valid(1)"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_is_missing_value_error(self):
        expr = "age.is_missing(1)"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_is_missing_arrays(self):
        expr = "age.is_missing([1], [2])"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_unknown_function(self):
        expr = "other()"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_wrong_ops(self):
        expr = "a == 1 == 1"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_unsupported_args(self):
        expr = "valid(starargs=1)"
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_expr_none(self):
        expr = None
        result = parse_expr(expr)
        assert result == {}

    def test_process_nested(self):
        expr = '(identity == 1 and caseid <= surveyid) or identity >= 2'
        variables = {
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
        table_mock = mock.MagicMock(metadata=variables)
        ds = mock.MagicMock()
        ds.self = 'http://host.com/api/datasets/abc123/'
        ds.follow.return_value = table_mock
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
        variables = {
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

        table_mock = mock.MagicMock(metadata=variables)
        ds = mock.MagicMock()
        ds.self = 'http://host.com/api/datasets/abc123/'
        ds.follow.return_value = table_mock
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

    def test_parse_float_value_in_list(self):
        expr = "country_cat in [1.0]"
        expected = {
            'function': 'in',
            'args': [
                {
                    'variable': 'country_cat'
                },
                {
                    'value': [1.0]
                }
            ]
        }
        expr_obj = parse_expr(expr)
        assert expr_obj == expected

    def test_parse_integer_value_in_list(self):
        expr = "country_cat in [1]"
        expected = {
            'function': 'in',
            'args': [
                {
                    'variable': 'country_cat'
                },
                {
                    'value': [1]
                }
            ]
        }
        expr_obj = parse_expr(expr)
        assert expr_obj == expected

    def test_r_in(self):
        expr = "q1 in [1, 2, r(4,7), r(10, 12)]"
        expected_expr_obj = {
            'args':
                [
                    {'variable': 'q1'},
                    {'value': [1, 2, 4, 5, 6, 7, 10, 11, 12]}
                ],
            'function': 'in'

        }
        expr_obj = parse_expr(expr)
        assert expr_obj == expected_expr_obj

    def test_r_raise(self):
        with pytest.raises(AttributeError) as excinfo:
            parse_expr("q1 in [r(10, )]")
        assert "function 'r' needs 2 integer arguments" in str(excinfo.value)

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

    def test_parse_any(self):
        expr = 'Q2.any([1, 2, 3])'
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

        expr = 'Q2.any((1, 2, 3))'
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

        expr = 'Q2.any(1)'
        with pytest.raises(ValueError):
            parse_expr(expr)

        expr = 'Q2.any(Q3)'
        with pytest.raises(ValueError):
            parse_expr(expr)

    def test_parse_all(self):
        expr = 'Q2.all([1, 2, 3])'
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

        expr = 'Q2.all((1, 2, 3))'
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

        expr = 'Q2.all(1)'
        with pytest.raises(ValueError):
            parse_expr(expr)

        expr = 'Q2.all(Q3)'
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
            }, {
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

    def test_mr_any_subvar(self):
        expr = "MyMrVar.any([subvar1, subvar2])"
        parsed_zcl_expr = parse_expr(expr)
        assert parsed_zcl_expr == {
            'function': 'any',
            'args': [
                {'variable': 'MyMrVar'},
                {'column': ['subvar1', 'subvar2']}
            ]
        }

    def test_mr_all_subvar(self):
        expr = "MyMrVar.all([subvar1, subvar2])"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'all',
            'args': [
                {'variable': 'MyMrVar'},
                {'column': ['subvar1', 'subvar2']}
            ]
        }

    def test_mr_in_subvar(self):
        expr = "MyMrVar in [subvar1, subvar2]"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {'variable': 'MyMrVar'},
                {'column': ['subvar1', 'subvar2']}
            ]
        }

    def test_parse_sample_any(self):
        # 'text': 'CompanyTurnover is NA',
        # 'index_mapper': {'CompanyTurnover': any([99])}},

        # 'text': 'Not Private Sector',
        # 'index_mapper': {'sector': any([2, 3, 98, 99])}},
        expr = "CompanyTurnover.any([99])"
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

        expr = "sector.any([2, 3, 98, 99])"
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
        expr = 'not Q2.any([1, 2, 3])'
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

        expr = 'not Q2.all([1, 2, 3])'
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

    def test_multiple_arithmetic_operations(self):
        expr = "1 + 2 * 3"
        expr_obj = parse_expr(expr)

        assert expr_obj == {
            'function': '+', 'args': [
                {'value': 1},
                {
                    'function': '*',
                    'args': [
                        {'value': 2},
                        {'value': 3}
                    ]
                },
            ]
        }

    def test_multiple_arithmetic_operations_precedence(self):
        expr = "1 + 2 / 3 - 4 * 5"
        expr_obj = parse_expr(expr)

        assert expr_obj == {
            'function': '-',
            'args': [
                {
                    'function': '+',
                    'args': [
                        {'value': 1},
                        {
                            'function': '/',
                            'args': [
                                {'value': 2},
                                {'value': 3}]
                        }
                    ]
                },
                {
                    'function': '*',
                    'args': [
                        {'value': 4},
                        {'value': 5}
                    ]
                }
            ]
        }

    def test_multiple_arithmetic_operations_with_variable(self):
        expr = "(weekly_rent * 52) + 12"
        expr_obj = parse_expr(expr)

        assert expr_obj == {
            "function": "+",
            "args": [
                {
                    "function": "*",
                    "args": [
                        {
                            "variable": "weekly_rent"
                        },
                        {
                            "value": 52
                        }
                    ]
                },
                {
                    "value": 12
                }
            ]
        }

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
            'function': 'or',
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
            'function': 'or',
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
                    'function': 'or',
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
                    'function': 'or',
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
                    'function': 'or',
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

    def test_multiple_missing_valid(self):
        # More than 2 variables.
        expr = "missing(year, month, age)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'args': [
                {
                    'args': [{'variable': 'year'}],
                    'function': 'is_missing'
                },
                {
                    'args': [
                        {
                            'args': [{'variable': 'month'}],
                            'function': 'is_missing'
                        },
                        {
                            'args': [{'variable': 'age'}],
                            'function': 'is_missing'
                        }
                    ],
                    'function': 'or'
                }
            ],
            'function': 'or'
        }

        expr = "valid(year, month, age, gender)"
        expr_obj = parse_expr(expr)
        assert expr_obj == {
            'args': [
                {
                    'args': [{'variable': 'year'}],
                    'function': 'is_valid'
                },
                {
                    'args': [
                        {
                            'args': [{'variable': 'month'}],
                            'function': 'is_valid'
                        },
                        {
                            'args': [
                                {
                                    'args': [{'variable': 'age'}],
                                    'function': 'is_valid'
                                },
                                {
                                    'args': [{'variable': 'gender'}],
                                    'function': 'is_valid'
                                }
                            ],
                            'function': 'or'
                        }
                    ],
                    'function': 'or'
                }
            ],
            'function': 'or'
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

    def test_parse_subvariable_brackets(self):
        expr = "array_alias[subvariable_alias] in [1, 2, 3]"
        expr_obj = parse_expr(expr, platonic=False)
        assert expr_obj == {
            'function': 'in',
            'args': [
                # Note how instead of storing a variable string as identifier
                # this is a temporary intern format, so we can use this later
                # on to convert to URLs appropriately discovering first the
                # array and then the subvariable
                {'variable': {"array": "array_alias", "subvariable": "subvariable_alias"}},
                {'value': [1, 2, 3]}
            ]
        }
        expr_obj = parse_expr(expr, platonic=True)
        assert expr_obj == {
            'function': 'in',
            'args': [
                # Note how instead of storing a variable string as identifier
                # this is a temporary intern format, so we can use this later
                # on to convert to URLs appropriately discovering first the
                # array and then the subvariable
                {'var': "array_alias", "axes": ["subvariable_alias"]},
                {'value': [1, 2, 3]}
            ]
        }

    def test_parse_platonic_expr(self):
        expr = """not (array[subvar] or num_val) and other[dimension] and not logical"""
        parsed = parse_expr(expr, platonic=True)
        assert parsed == {
            'function': 'and',
            'args': [
                {
                    'function': 'not',
                    'args': [
                        {
                            'function': 'or',
                            'args': [
                                {'var': 'array', 'axes': ['subvar']},
                                {'var': 'num_val'}
                            ],
                        }
                    ],
                },
                {
                    'function': 'and',
                    'args': [
                        {'var': 'other', 'axes': ['dimension']},
                        {
                            'function': 'not',
                            'args': [{'var': 'logical'}],
                        }
                    ],
                }
            ],
        }


# 'diposition code 0 (incompletes)':
# intersection(
#     [{'disposition': not_any([1])},
#      union([{'exit_status': has_count(0)},
#             {'exit_status': is_ge(1)}])
#      ]
# )

# 'text': 'sta: nicht aus Deutschland',
# 'index_mapper': {'sta': any([17])}},

# 'text': '(age >= 18) & profile_julesage is NaN',
#             'index_mapper': intersection(
#                 [
#                     {'age': is_ge(18)},
#                     {'profile_julesage': has_count(0)}])},

# 'text': '(age >= 18) & profile_bpcagesex is NaN',
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

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

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

    @mark_fail_py2
    def test_adapt_multiple_response_any_subvar(self):
        var_id = '0001'
        var_alias = 'MyMrVar'
        var_type = 'multiple_response'
        var_url = '{}variables/{}/'.format(self.ds_url, var_id)
        var_categories = [
                    {"id": 1, "name": "cat1", "selected": True},
                    {"id": 2, "name": "cat2", "selected": True},
                    {"id": 3, "name": "cat3", "selected": False},
                ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                "subvariables": [
                    "%ssubvariables/001/" % var_url,
                    "%ssubvariables/002/" % var_url,
                    "%ssubvariables/003/" % var_url,
                ],
                "subreferences": {
                    "%ssubvariables/001/" % var_url: {"alias": "subvar1"},
                    "%ssubvariables/002/" % var_url: {"alias": "subvar2"},
                    "%ssubvariables/003/" % var_url: {"alias": "subvar3"},
                },
                "categories": var_categories
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.variables.index = {
            "{}".format(var_url): {
                "name": "Multiple Response",
                "description": "",
                "notes": "",
                "alias": "mr_variable",
                "id": "{}".format(var_id),
                "type": "multiple_response",
                "subvariables": [
                    "{}subvariables/001/".format(var_url),
                    "{}subvariables/002/".format(var_url),
                    "{}subvariables/003/".format(var_url),
                ],
            }
        }

        ds.follow.return_value = table_mock
        values = ["subvar1", "subvar2"]
        with mock.patch("scrunch.expressions.get_subvariables_resource") as mock_subvars, mock.patch("scrunch.expressions._get_categories_from_var_index") as categories:
            categories.return_value = var_categories
            mock_subvars.return_value = dict(sorted({"subvar1": "001", "subvar2": "002", "subvar3": "003"}.items()))
            result, need_wrap = adapt_multiple_response(var_url, values, ds.variables.index)
            assert result == [
                {'variable': "{}subvariables/001/".format(var_url), 'column': [1, 2]},
                {'variable': "{}subvariables/002/".format(var_url), 'column': [1, 2]}
            ]
            assert need_wrap is True

    def test_process_all_multiple_response(self):
        var_id = '0001'
        var_alias = 'MyMrVar'
        var_type = 'multiple_response'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        var_categories = [
            {"id": 1, "name": "cat1", "selected": True},
            {"id": 2, "name": "cat2", "selected": True},
            {"id": 3, "name": "cat3", "selected": False},
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                "subvariables": [
                    "%ssubvariables/001/" % var_url,
                    "%ssubvariables/002/" % var_url,
                    "%ssubvariables/003/" % var_url,
                ],
                "subreferences": {
                    "%ssubvariables/001/" % var_url: {"alias": "subvar1"},
                    "%ssubvariables/002/" % var_url: {"alias": "subvar2"},
                    "%ssubvariables/003/" % var_url: {"alias": "subvar3"},
                },
                "categories": var_categories
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.variables.index = {
            "{}".format(var_url): {
                "name": "Multiple Response",
                "description": "",
                "notes": "",
                "alias": "mr_variable",
                "id": "{}".format(var_id),
                "type": "multiple_response",
                "subvariables": [
                    "{}subvariables/001/".format(var_url),
                    "{}subvariables/002/".format(var_url),
                    "{}subvariables/003/".format(var_url),
                ],
            }
        }

        ds.follow.return_value = table_mock
        expr = "MyMrVar.all([1])"
        with mock.patch("scrunch.expressions.get_subvariables_resource") as mock_subvars, mock.patch(
                "scrunch.expressions._get_categories_from_var_index") as categories:
            categories.return_value = var_categories
            mock_subvars.return_value = dict(sorted({"subvar1": "001", "subvar2": "002", "subvar3": "003"}.items()))
            parsed_expr = parse_expr(expr)
            processed_zcl_expr = process_expr(parsed_expr, ds)
            assert sorted(processed_zcl_expr) == sorted({
                'function': 'and',
                'args': [
                    {
                        'function': '==',
                        'args': [
                            {'variable': "{}subvariables/001/".format(var_url)},
                            {'value': 1}
                        ],
                    },
                    {
                        'function': '==',
                        'args': [
                            {'variable': "{}subvariables/002/".format(var_url)},
                            {'value': 1}
                        ],
                    },
                    {
                        'function': '==',
                        'args': [
                            {'variable': "{}subvariables/003/".format(var_url)},
                            {'value': 1}
                        ],
                    }
                ],
            })

    @pytest.mark.xfail(reason="multiple response with `in` is not yet supported")
    def test_process_in_multiple_response(self):
        # TODO: check how to handle this scenario in future releases. This should work as .any
        var_id = '0001'
        var_alias = 'MyMrVar'
        var_type = 'multiple_response'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        var_categories = [
            {"id": 1, "name": "cat1", "selected": True},
            {"id": 2, "name": "cat2", "selected": True},
            {"id": 3, "name": "cat3", "selected": False},
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                "subvariables": [
                    "%ssubvariables/001/" % var_url,
                    "%ssubvariables/002/" % var_url,
                    "%ssubvariables/003/" % var_url,
                ],
                "subreferences": {
                    "%ssubvariables/001/" % var_url: {"alias": "subvar1"},
                    "%ssubvariables/002/" % var_url: {"alias": "subvar2"},
                    "%ssubvariables/003/" % var_url: {"alias": "subvar3"},
                },
                "categories": var_categories
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.variables.index = {
            "{}".format(var_url): {
                "name": "Multiple Response",
                "description": "",
                "notes": "",
                "alias": "mr_variable",
                "id": "{}".format(var_id),
                "type": "multiple_response",
                "subvariables": [
                    "{}subvariables/001/".format(var_url),
                    "{}subvariables/002/".format(var_url),
                    "{}subvariables/003/".format(var_url),
                ],
            }
        }

        ds.follow.return_value = table_mock
        expr = "MyMrVar in [1]"
        with mock.patch("scrunch.expressions.get_subvariables_resource") as mock_subvars, mock.patch(
                "scrunch.expressions._get_categories_from_var_index") as categories:
            categories.return_value = var_categories
            mock_subvars.return_value = dict(sorted({"subvar1": "001", "subvar2": "002", "subvar3": "003"}.items()))
            parsed_expr = parse_expr(expr)
            processed_zcl_expr = process_expr(parsed_expr, ds)
            assert processed_zcl_expr == {
                'function': 'or',
                'args': [
                    {
                        'function': 'in',
                        'args': [
                            {'variable': "{}subvariables/001/".format(var_url)},
                            {'column': [1]}
                        ],
                    },
                    {
                        'function': 'in',
                        'args': [
                            {'variable': "{}subvariables/002/".format(var_url)},
                            {'column': [1]}
                        ],
                    },
                    {
                        'function': 'in',
                        'args': [
                            {'variable': "{}subvariables/003/".format(var_url)},
                            {'column': [1]}
                        ],
                    }
                ],
            }

    def test_multiple_response_any_process_single_subvariables(self):
        var_id = '0001'
        var_alias = 'MyMrVar'
        var_type = 'multiple_response'
        var_url = '{}variables/{}/'.format(self.ds_url, var_id)
        var_categories = [
            {"id": 1, "name": "cat1", "selected": True},
            {"id": 2, "name": "cat2", "selected": False},
            {"id": 3, "name": "cat3", "selected": False},
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                "subvariables": [
                    "001",
                    "002",
                    "003",
                ],
                "subreferences": {
                    "001": {"alias": "subvar1"},
                    "002": {"alias": "subvar2"},
                    "003": {"alias": "subvar3"},
                },
                "categories": var_categories
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.variables.index = {
            "{}".format(var_url): {
                "name": "Multiple Response",
                "description": "",
                "notes": "",
                "alias": "mr_variable",
                "id": "{}".format(var_id),
                "type": "multiple_response",
                "subvariables": [
                    "001".format(var_url),
                    "002".format(var_url),
                    "003".format(var_url),
                ],
                "entity": {
                    "subvariables": {
                        "index": {
                            "001": {
                                "id": "001",
                                "alias": "subvar1"
                            },
                            "002": {
                                "id": "002",
                                "alias": "subvar2"
                            },
                            "003": {
                                "id": "003",
                                "alias": "subvar3"
                            }
                        }
                    }
                }
            }
        }
        ds.follow.return_value = table_mock
        expr = "MyMrVar.any([subvar1])"
        parsed_expr = parse_expr(expr)
        with mock.patch("scrunch.expressions.get_subvariables_resource") as mock_subvars, mock.patch(
                "scrunch.expressions._get_categories_from_var_index") as categories:
            categories.return_value = var_categories
            mock_subvars.return_value = dict(sorted({"subvar1": "001", "subvar2": "002", "subvar3": "003"}.items()))
            processed_zcl_expr = process_expr(parsed_expr, ds)
        assert processed_zcl_expr == {
            'function': 'in',
            'args': [
                {
                    'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/001/'
                },
                {
                    'column': [1]
                }
            ],
        }

    @mark_fail_py2 
    def test_multiple_response_any_process_two_subvariables(self):
        var_id = '0001'
        var_alias = 'MyMrVar'
        var_type = 'multiple_response'
        var_url = '{}variables/{}/'.format(self.ds_url, var_id)
        var_categories = [
            {"id": 1, "name": "cat1", "selected": True},
            {"id": 2, "name": "cat2", "selected": False},
            {"id": 3, "name": "cat3", "selected": False},
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                "subvariables": [
                    "001",
                    "002",
                    "003",
                ],
                "subreferences": {
                    "001": {"alias": "subvar1"},
                    "002": {"alias": "subvar2"},
                    "003": {"alias": "subvar3"},
                },
                "categories": var_categories
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.variables.index = {
            "{}".format(var_url): {
                "name": "Multiple Response",
                "description": "",
                "notes": "",
                "alias": "mr_variable",
                "id": "{}".format(var_id),
                "type": "multiple_response",
                "subvariables": [
                    "001",
                    "002".format(var_url),
                    "003".format(var_url),
                ],
                "entity": {
                    "subvariables": {
                        "index": {
                            "001": {
                                "id": "001",
                                "alias": "subvar1"
                            },
                            "002": {
                                "id": "002",
                                "alias": "subvar2"
                            },
                            "003": {
                                "id": "003",
                                "alias": "subvar3"
                            }
                        }
                    }
                }
            }
        }
        ds.follow.return_value = table_mock
        expr = "MyMrVar.any([subvar1, subvar2])"
        parsed_expr = parse_expr(expr)
        with mock.patch("scrunch.expressions.get_subvariables_resource") as mock_subvars, mock.patch("scrunch.expressions._get_categories_from_var_index") as categories:
            categories.return_value = var_categories
            mock_subvars.return_value = dict(sorted({"subvar1": "001", "subvar2": "002", "subvar3": "003"}.items()))
            processed_zcl_expr = process_expr(parsed_expr, ds)
        assert processed_zcl_expr == {
            'function': 'or',
            'args': [
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/001/'
                        },
                        {
                            'column': [1]
                        }
                    ],
                },
                {
                    'function': 'in',
                    'args': [
                        {
                            'variable': 'http://test.crunch.io/api/datasets/123/variables/0001/subvariables/002/'
                        },
                        {
                            'column': [1]
                        }
                    ],
                },
            ]
        }

    def test_multiple_response_subvar_equality(self):
        var_id = '0001'
        var_alias = 'MyMrVar'
        var_type = 'multiple_response'
        var_url = '{}variables/{}/'.format(self.ds_url, var_id)
        var_categories = [
            {"id": 1, "name": "cat1", "selected": True},
            {"id": 2, "name": "cat2", "selected": False},
            {"id": 3, "name": "cat3", "selected": False},
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                "subvariables": [
                    "001",
                    "002",
                    "003",
                ],
                "subreferences": {
                    "001": {"alias": "subvar1"},
                    "002": {"alias": "subvar2"},
                    "003": {"alias": "subvar3"},
                },
                "categories": var_categories
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.variables.index = {
            "{}".format(var_url): {
                "name": "Multiple Response",
                "description": "",
                "notes": "",
                "alias": "mr_variable",
                "id": "{}".format(var_id),
                "type": "multiple_response",
                "subvariables": [
                    "{}subvariables/001/".format(var_url),
                    "{}subvariables/002/".format(var_url),
                    "{}subvariables/003/".format(var_url),
                ],
                "entity": {
                    "subvariables": {
                        "index": {
                            "001": {
                                "id": "001",
                                "alias": "subvar1"
                            },
                            "002": {
                                "id": "002",
                                "alias": "subvar2"
                            },
                            "003": {
                                "id": "003",
                                "alias": "subvar3"
                            }
                        }
                    }
                }
            }
        }
        ds.follow.return_value = table_mock
        expr = 'subvar1 == 1'
        parsed_expr = parse_expr(expr)
        expr_obj = process_expr(parsed_expr, ds)

        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'variable': "{}subvariables/001/".format(var_url),
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

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

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

    def test_transform_subvar_alias_w_brackets_to_subvar_id(self):
        var_id = '0001'
        var_alias = 'hobbies_array'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = ['0001', '0002']
        subreferences = {'0001': {'alias': 'hobbies_1'}, '0002': {'alias': 'hobbies_2'}}

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock
        subvariable_url = '%ssubvariables/%s/' % (var_url, subvariables[0])

        # Expression with subvariable indicated by bracket syntax
        expr = "hobbies_array[hobbies_1] == 4"
        parsed_platonic = parse_expr(expr, platonic=True)
        assert parsed_platonic == {
            'function': '==',
            'args': [
                # Keeps the platonic reference to the subvariable
                {'var': 'hobbies_array', 'axes': ['hobbies_1']},
                {'value': 4}
            ]
        }
        parsed = parse_expr(expr)
        assert parsed == {
            'function': '==',
            'args': [
                # Stores a reference to the pieces of the array/subvariable
                {"variable": {"array": 'hobbies_array', "subvariable": 'hobbies_1'}},
                {'value': 4}
            ]
        }
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': '==',
            'args': [
                # Correctly translates into the subvariable URL
                {'variable': subvariable_url},
                {'value': 4}
            ]
        }

        # Expression with subvariable indicated by bracket syntax
        expr = "hobbies_array[hobbies_1].any([1, 2])"
        parsed_platonic = parse_expr(expr, platonic=True)
        assert parsed_platonic == {
            'function': "any",
            'args': [
                # Platonic parsing keeps the var/axes reference
                {'var': 'hobbies_array', 'axes': ['hobbies_1']},
                {'value': [1, 2]}
            ]
        }
        parsed = parse_expr(expr)
        assert parsed == {
            'function': "any",
            'args': [
                # Stores a reference to the array/subvairable
                {"variable": {"array": 'hobbies_array', "subvariable": 'hobbies_1'}},
                {'value': [1, 2]}
            ]
        }
        expr_obj = process_expr(parsed, ds)
        assert expr_obj == {
            'function': "in",
            'args': [
                # Still finds the correct subvariable ID under the array URL
                {'variable': subvariable_url},
                {'value': [1, 2]}
            ]
        }

        # `IN` functions have a bit of a special treatment.
        expr = "hobbies_array[hobbies_1] in [1]"
        parsed_platonic = parse_expr(expr, platonic=True)
        assert parsed_platonic == {
            'function': 'in',
            'args': [
                # Keeps the platonic reference to the subvariable
                {'var': 'hobbies_array', 'axes': ['hobbies_1']},
                {'value': [1]}
            ]
        }
        parsed = parse_expr(expr)
        assert parsed == {
            'function': 'in',
            'args': [
                # Stores a reference to the pieces of the array/subvariable
                {"variable": {"array": 'hobbies_array', "subvariable": 'hobbies_1'}},
                {'value': [1]}
            ]
        }
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                # Correctly translates into the subvariable URL
                {'variable': subvariable_url},
                {'value': [1]}
            ]
        }

    def test_platonic_filter(self):
        var_id = '0001'
        var_alias = 'hobbies_array'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = ['0001', '0002']
        subreferences = {'0001': {'alias': 'hobbies_1'}, '0002': {'alias': 'hobbies_2'}}

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock
        subvariable_url = '%ssubvariables/%s/' % (var_url, subvariables[0])

        # Expression with subvariable indicated by bracket syntax
        expr = "hobbies_array[hobbies_1] == 4"
        parsed = parse_expr(expr, platonic=True)
        assert parsed == {
            'function': '==',
            'args': [
                # Keeps the platonic reference to the subvariable
                {'var': 'hobbies_array', 'axes': ['hobbies_1']},
                {'value': 4}
            ]
        }
        expr_obj = process_expr(parsed, ds)
        assert expr_obj == parsed

        parsed = parse_expr(expr, platonic=False)
        assert parsed == {
            'function': '==',
            'args': [
                # Keeps the platonic reference to the subvariable
                {"variable": {"array": 'hobbies_array', "subvariable": 'hobbies_1'}},
                {'value': 4}
            ]
        }
        expr_obj = process_expr(parsed, ds)
        assert expr_obj == {
            'function': '==',
            'args': [
                # Keeps the platonic reference to the subvariable
                {"variable": subvariable_url},
                {'value': 4}
            ]
        }

    def test_array_expansion_single_subvariable_any(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001'
        ]

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        # Single value.
        expr_obj = process_expr(parse_expr('hobbies.any([32766])'), ds)
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

    def test_array_expansion_single_subvariable_all(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001'
        ]

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr_obj = process_expr(parse_expr('hobbies.all([32766])'), ds)
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

    def test_array_expansion_single_subvariable_not_any(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001'
        ]

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        # Negated.
        expr_obj = process_expr(parse_expr('not hobbies.any([32766])'), ds)
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

    def test_array_expansion_single_subvariable_not_all(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001'
        ]

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr_obj = process_expr(parse_expr('not hobbies.all([32766])'), ds)
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

    def test_array_expansion_single_subvariable_multiple_any(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        subvariables = [
            '0001'
        ]

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        # Multiple values.
        expr_obj = process_expr(parse_expr('hobbies.any([32766, 32767])'), ds)
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

    def test_array_expansion_single_subvariable_multiple_all(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical_array'
        subvariables = [
            '0001'
        ]

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        with pytest.raises(ValueError):
            process_expr(parse_expr('hobbies.all([32766, 32767])'), ds)

    def test_categorical_array_any_expansion_multiple_subvariables(self):
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
        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
            '0003': {'alias': 'hobbies_3'},
            '0004': {'alias': 'hobbies_4'}
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        # Single values.
        expr = 'hobbies.any([32766])'
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

    def test_categorical_array_all_process_expression(self):
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
        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
            '0003': {'alias': 'hobbies_3'},
            '0004': {'alias': 'hobbies_4'}
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = 'hobbies.all([32766])'
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

    def test_categorical_array_not_any_process_expression(self):
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
        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
            '0003': {'alias': 'hobbies_3'},
            '0004': {'alias': 'hobbies_4'}
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        # Negated.
        expr = 'not hobbies.any([32766])'
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

    def test_categorical_array_not_all_process_expression(self):
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
        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
            '0003': {'alias': 'hobbies_3'},
            '0004': {'alias': 'hobbies_4'}
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = 'not hobbies.all([32766])'
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

    def test_categorical_array_any_multiple_selection_process_expression(self):
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
        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
            '0003': {'alias': 'hobbies_3'},
            '0004': {'alias': 'hobbies_4'}
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        # Multiple values.
        expr = 'hobbies.any([32766, 32767])'
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

    def test_categorical_array_not_any_multiple_selection_process_expression(self):
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
        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
            '0003': {'alias': 'hobbies_3'},
            '0004': {'alias': 'hobbies_4'}
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        # Multiple values, negated
        expr = 'not hobbies.any([32766, 32767])'
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

        subreferences = {
            '0001': {'alias': 'hobbies_1'},
            '0002': {'alias': 'hobbies_2'},
            '0003': {'alias': 'hobbies_3'},
            '0004': {'alias': 'hobbies_4'}
        }

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': [],
                'subvariables': subvariables,
                'subreferences': subreferences
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

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
            'function': 'is_missing',
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
                    'function': 'is_missing',
                    'args': [
                        {
                            'variable': var_url
                        }
                    ]
                }
            ]
        }

    def test_label_expression_single(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        categories = [
            {
                'name': 'mocking',
                'id': 1
            }
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': categories,
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = "hobbies == 'mocking'"
        expr_obj = process_expr(parse_expr(expr), ds)
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

    def test_label_expression_list(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        categories = [
            {
                'name': 'mocking',
                'id': 1
            },
            {
                'name': 'coding',
                'id': 2
            },
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': categories,
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = "hobbies in ['mocking', 'coding']"
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': var_url
                },
                {
                    'value': [1, 2]
                }
            ]
        }

    def test_label_expression_tuple(self):
        var_id = '0001'
        var_alias = 'hobbies'
        var_type = 'categorical'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        categories = [
            {
                'name': 'mocking',
                'id': 1
            },
            {
                'name': 'coding',
                'id': 2
            },
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': categories,
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = "hobbies in ('mocking', 'coding')"
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': var_url
                },
                {
                    'value': [1, 2]
                }
            ]
        }

    def test_any_categorical_var(self):
        var_id = '0001'
        var_alias = 'my_categorical'
        var_type = 'categorical'
        var_url = '{}variables/{}/'.format(self.ds_url, var_id)
        categories = [
            {
                'name': 'mocking',
                'id': 1
            },
            {
                'name': 'coding',
                'id': 2
            },
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': categories,
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = "my_categorical.any([1])"
        parsed_expr = parse_expr(expr)
        expr_obj = process_expr(parsed_expr, ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': var_url
                },
                {
                    'value': [1]
                }
            ]
        }

    def test_in_expression_list_integer(self):
        var_id = '0001'
        var_alias = 'country_cat'
        var_type = 'categorical'
        var_url = '{}variables/{}/'.format(self.ds_url, var_id)
        categories = [
            {
                'name': 'argentina',
                'id': 1
            },
            {
                'name': 'australia',
                'id': 2
            },
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': categories,
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = "country_cat in [1]"
        expr_obj = process_expr(parse_expr(expr), ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': var_url
                },
                {
                    'value': [1]
                }
            ]
        }

    def test_in_expression_list_floats(self):
        var_id = '0001'
        var_alias = 'country_cat'
        var_type = 'categorical'
        var_url = '{}variables/{}/'.format(self.ds_url, var_id)
        categories = [
            {
                'name': 'argentina',
                'id': 1
            },
            {
                'name': 'australia',
                'id': 2
            },
        ]

        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': categories,
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock

        expr = "country_cat in [1.0]"
        parsed_expr = parse_expr(expr)
        expr_obj = process_expr(parsed_expr, ds)
        assert expr_obj == {
            'function': 'in',
            'args': [
                {
                    'variable': var_url
                },
                {
                    'value': [1.0]
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

    def test_float_conversion_to_integer(self):
        expr = {
            'function': '==',
            'args': [
                {
                    'variable': 'age'
                },
                {
                    'value': 25.0
                }
            ]
        }

        expected = 'age == 25'
        cel = prettify(expr)
        assert expected == cel

    def test_float_conversion_integer_in_list(self):
        expr = {
            "function": "in",
            "args": [
                {
                    "variable": "my_var"
                },
                {
                    "value": [
                        1.0, 2.0
                    ]
                }
            ]
        }
        assert prettify(expr) == "my_var in [1, 2]"

    def test_string_no_need_conversion_in_list(self):
        expr = {
            "function": "in",
            "args": [
                {
                    "variable": "my_var"
                },
                {
                    "value": [
                        "test"
                    ]
                }
            ]
        }
        assert prettify(expr) == "my_var in ['test']"

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
        ds.__class__ = scrunch.mutable_dataset.MutableDataset
        response = mock.MagicMock()
        response.payload.body = {"alias": "age"}

        ds.resource.session.get.side_effect = lambda *arg: response

        expected = 'age == 1'
        cel = prettify(expr, ds)
        assert expected == cel
        ds.resource.session.get.assert_called_with('https://host.com/api/datasets/123/variables/001/')

    def test_square_bracket_subvariables(self):
        subvariable_url = 'https://host.com/api/datasets/123/variables/001/subvariables/abc/'
        expr = {
            'function': '==',
            'args': [
                {
                    'variable': subvariable_url
                },
                {
                    'value': 1
                }
            ]
        }

        ds = mock.MagicMock()
        ds.__class__ = scrunch.mutable_dataset.MutableDataset

        # Prepare subvariable
        subvar_resource = mock.MagicMock()
        subvar_resource.catalogs = {"parent": "/subvariables/"}
        subvar_resource.fragments = {"variable": "/array_url/"}
        subvar_resource.body = {"alias": "subvar_1"}

        response1 = mock.MagicMock()
        response1.payload = subvar_resource

        # Prepare array
        response2 = mock.MagicMock()
        response2.payload.body = {"alias": 'array_variable'}
        ds.resource.session.get.side_effect = [response1, response2]

        expected = 'array_variable[subvar_1] == 1'
        assert prettify(expr, ds) == expected

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
        assert cel == '(identity == 1 and caseid <= surveyid) or identity >= 2'

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

    def test_parse_any(self):
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
        assert cel == 'Q2.any([1, 2, 3])'

        # Works with subvariable aliases
        expr = {
            'function': 'any',
            'args': [
                {'variable': 'Q2'},
                {'value': ["subvar1", "subvar2"]}
            ]
        }
        cel = prettify(expr)
        assert cel == "Q2.any(['subvar1', 'subvar2'])"

    def test_parse_all(self):
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
        assert cel == 'Q2.all([1, 2, 3])'

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
            }, {
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

    def test_parse_sample_any(self):
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
        assert cel == "CompanyTurnover.any([99])"

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
        assert cel == "sector.any([2, 3, 98, 99])"

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
        assert cel == 'not Q2.any([1, 2, 3])'

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
        assert cel == 'not Q2.all([1, 2, 3])'

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


class TestDatetimeStrings(TestCase):

    def test_iso8601_complete(self):
        expr = "starttime < '2016-12-21T12:00:00+00:00'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016-12-21T12:00:00+00:00"
                }
            ]
        }

    def test_iso8601_wo_tzinfo(self):
        expr = "starttime < '2016-12-21T12:00:00'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016-12-21T12:00:00"
                }
            ]
        }

    def test_iso8601_day_hour_minute_sec(self):
        expr = "starttime < '2016-12-21T12:00:00'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016-12-21T12:00:00"
                }
            ]
        }

    def test_iso8601_day_hour_minute(self):
        expr = "starttime < '2016-12-21T12:00'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016-12-21T12:00"
                }
            ]
        }

    def test_iso8601_day_hour(self):
        expr = "starttime < '2016-12-21T12'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016-12-21T12"
                }
            ]
        }

    def test_iso8601_day(self):
        expr = "starttime < '2016-12-21'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016-12-21"
                }
            ]
        }

    def test_iso8601_month(self):
        expr = "starttime < '2016-12'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016-12"
                }
            ]
        }

    def test_iso8601_year(self):
        expr = "starttime < '2016'"
        assert parse_expr(expr) == {
            "function": "<",
            "args": [
                {
                    "variable": "starttime"
                },
                {
                    "value": "2016"
                }
            ]
        }


class TestDateTimeExpression(TestCase):
    """
    Test for datetimes being correctly interpreted as values
    """

    ds_url = 'http://test.crunch.io/api/datasets/12345/'

    def mock_dataset(self, var_id, var_alias, var_type, categories=None):
        """
        Helper for mocking a dataset for datetimes testing
        """
        if not categories:
            categories = []
        table_mock = mock.MagicMock(metadata={
            var_id: {
                'id': var_id,
                'alias': var_alias,
                'type': var_type,
                'categories': categories
            }
        })
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock
        return ds

    def test_process_expression(self):
        var_id = '0001'
        var_alias = 'starttime'
        var_type = 'datetime'
        var_url = '%svariables/%s/' % (self.ds_url, var_id)
        ds = self.mock_dataset(var_id, var_alias, var_type)
        expr = "starttime < '2016-12-21'"
        parsed = parse_expr(expr)
        expr_obj = process_expr(parsed, ds)
        assert expr_obj == {
            'function': '<',
            'args': [
                {
                    'variable': var_url
                },
                {
                    'value': '2016-12-21'
                }
            ]
        }

    def test_datetime_as_value(self):
        ds = self.mock_dataset(None, '', '')
        expr = "'2016-12-21T12' == 5"
        parsed = parse_expr(expr)
        expr_obj = process_expr(parsed, ds)
        assert expr_obj == {
            'function': '==',
            'args': [
                {
                    'value': '2016-12-21T12'
                },
                {
                    'value': 5
                }
            ]
        }

class TestGetDatasetVariables(TestCase):
    ds_url = "http://mock.crunch.io/api/datasets/123/"

    def test_get_dataset_variables_categorical_arrays(self):
        var_id = "0001"
        var_alias = "hobbies"
        var_type = "categorical_array"
        subvariables = ["0001", "0002", "0003", "0004"]
        subreferences = {
            "0001": {"alias": "hobbies_1"},
            "0002": {"alias": "hobbies_2"},
            "0003": {"alias": "hobbies_3"},
            "0004": {"alias": "hobbies_4"},
        }

        table_mock = mock.MagicMock(
            metadata={
                var_id: {
                    "id": var_id,
                    "alias": var_alias,
                    "type": var_type,
                    "categories": [
                        {"id": 1, "name": "cat1", "selected": True},
                        {"id": 2, "name": "cat2", "selected": True},
                        {"id": 3, "name": "cat3", "selected": False},
                    ],
                    "subvariables": subvariables,
                    "subreferences": subreferences,
                }
            }
        )
        ds = mock.MagicMock()
        ds.follow.return_value = table_mock
        result = get_dataset_variables(ds)
        assert result == {
            "hobbies[hobbies_1]": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_1",
                "is_subvar": True,
                "type": "categorical",
                "id": "0001",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies_3": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_3",
                "is_subvar": True,
                "type": "categorical",
                "id": "0003",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies[hobbies_4]": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_4",
                "is_subvar": True,
                "type": "categorical",
                "id": "0004",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies_4": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_4",
                "is_subvar": True,
                "type": "categorical",
                "id": "0004",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies_1": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_1",
                "is_subvar": True,
                "type": "categorical",
                "id": "0001",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies_2": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_2",
                "is_subvar": True,
                "type": "categorical",
                "id": "0002",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies": {
                "subreferences": {
                    "0004": {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "hobbies_4",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "0004",
                        "categories": [
                            {"selected": True, "id": 1, "name": "cat1"},
                            {"selected": True, "id": 2, "name": "cat2"},
                            {"selected": False, "id": 3, "name": "cat3"},
                        ],
                    },
                    "0001": {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "hobbies_1",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "0001",
                        "categories": [
                            {"selected": True, "id": 1, "name": "cat1"},
                            {"selected": True, "id": 2, "name": "cat2"},
                            {"selected": False, "id": 3, "name": "cat3"},
                        ],
                    },
                    "0002": {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "hobbies_2",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "0002",
                        "categories": [
                            {"selected": True, "id": 1, "name": "cat1"},
                            {"selected": True, "id": 2, "name": "cat2"},
                            {"selected": False, "id": 3, "name": "cat3"},
                        ],
                    },
                    "0003": {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "hobbies_3",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "0003",
                        "categories": [
                            {"selected": True, "id": 1, "name": "cat1"},
                            {"selected": True, "id": 2, "name": "cat2"},
                            {"selected": False, "id": 3, "name": "cat3"},
                        ],
                    },
                },
                "alias": "hobbies",
                "subvariables": ["0001", "0002", "0003", "0004"],
                "type": "categorical_array",
                "id": "0001",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies[hobbies_3]": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_3",
                "is_subvar": True,
                "type": "categorical",
                "id": "0003",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "hobbies[hobbies_2]": {
                "description": "",
                "parent_id": "0001",
                "alias": "hobbies_2",
                "is_subvar": True,
                "type": "categorical",
                "id": "0002",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
        }

    def test_get_dataset_variables_multiple_response(self):
        var_id = "0001"
        var_alias = "MyMrVar"
        var_type = "multiple_response"
        var_url = "{}variables/{}/".format(self.ds_url, var_id)
        var_categories = [
            {"id": 1, "name": "cat1", "selected": True},
            {"id": 2, "name": "cat2", "selected": True},
            {"id": 3, "name": "cat3", "selected": False},
        ]

        table_mock = mock.MagicMock(
            metadata={
                var_id: {
                    "id": var_id,
                    "alias": var_alias,
                    "type": var_type,
                    "subvariables": [
                        "{}subvariables/001/".format(var_url),
                        "{}subvariables/002/".format(var_url),
                        "{}subvariables/003/".format(var_url),
                    ],
                    "subreferences": {
                        "{}subvariables/001/".format(var_url): {"alias": "subvar1"},
                        "{}subvariables/002/".format(var_url): {"alias": "subvar2"},
                        "{}subvariables/003/".format(var_url): {"alias": "subvar3"},
                    },
                    "categories": var_categories,
                }
            }
        )
        ds = mock.MagicMock()

        ds.follow.return_value = table_mock
        result = get_dataset_variables(ds)
        assert result == {
            "MyMrVar[subvar2]": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar2",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/002/".format(var_url),
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "MyMrVar[subvar1]": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar1",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/001/".format(var_url),
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "subvar1": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar1",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/001/".format(var_url),
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "subvar2": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar2",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/002/".format(var_url),
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "subvar3": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar3",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/003/".format(var_url),
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "MyMrVar[subvar3]": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar3",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/003/".format(var_url),
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
            "MyMrVar": {
                "subreferences": {
                    "{}subvariables/003/".format(var_url): {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "subvar3",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "{}subvariables/003/".format(var_url),
                        "categories": [
                            {"selected": True, "id": 1, "name": "cat1"},
                            {"selected": True, "id": 2, "name": "cat2"},
                            {"selected": False, "id": 3, "name": "cat3"},
                        ],
                    },
                    "{}subvariables/001/".format(var_url): {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "subvar1",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "{}subvariables/001/".format(var_url),
                        "categories": [
                            {"selected": True, "id": 1, "name": "cat1"},
                            {"selected": True, "id": 2, "name": "cat2"},
                            {"selected": False, "id": 3, "name": "cat3"},
                        ],
                    },
                    "{}subvariables/002/".format(var_url): {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "subvar2",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "{}subvariables/002/".format(var_url),
                        "categories": [
                            {"selected": True, "id": 1, "name": "cat1"},
                            {"selected": True, "id": 2, "name": "cat2"},
                            {"selected": False, "id": 3, "name": "cat3"},
                        ],
                    },
                },
                "alias": "MyMrVar",
                "subvariables": [
                    "{}subvariables/001/".format(var_url),
                    "{}subvariables/002/".format(var_url),
                    "{}subvariables/003/".format(var_url),
                ],
                "type": "multiple_response",
                "id": "0001",
                "categories": [
                    {"selected": True, "id": 1, "name": "cat1"},
                    {"selected": True, "id": 2, "name": "cat2"},
                    {"selected": False, "id": 3, "name": "cat3"},
                ],
            },
        }

    def test_get_dataset_variables_numeric_arrays(self):
        var_id = "0001"
        var_alias = "num_arr_var"
        var_type = "numeric_array"
        var_url = "{}variables/{}/".format(self.ds_url, var_id)

        table_mock = mock.MagicMock(
            metadata={
                var_id: {
                    "id": var_id,
                    "alias": var_alias,
                    "type": var_type,
                    "subvariables": [
                        "{}subvariables/001/".format(var_url),
                        "{}subvariables/002/".format(var_url),
                        "{}subvariables/003/".format(var_url),
                    ],
                    "subreferences": {
                        "{}subvariables/001/".format(var_url): {"alias": "subvar1"},
                        "{}subvariables/002/".format(var_url): {"alias": "subvar2"},
                        "{}subvariables/003/".format(var_url): {"alias": "subvar3"},
                    },
                    "values": [
                        [1, 3, 1],
                        [2, 1, 1],
                    ],
                }
            }
        )
        ds = mock.MagicMock()
        ds.self = self.ds_url
        ds.follow.return_value = table_mock
        result = get_dataset_variables(ds)
        assert result == {
            "num_arr_var": {
                "subreferences": {
                    "{}subvariables/003/".format(var_url): {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "subvar3",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "{}subvariables/003/".format(var_url),
                    },
                    "{}subvariables/001/".format(var_url): {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "subvar1",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "{}subvariables/001/".format(var_url),
                    },
                    "{}subvariables/002/".format(var_url): {
                        "description": "",
                        "parent_id": "0001",
                        "alias": "subvar2",
                        "is_subvar": True,
                        "type": "categorical",
                        "id": "{}subvariables/002/".format(var_url),
                    },
                },
                "alias": "num_arr_var",
                "values": [[1, 3, 1], [2, 1, 1]],
                "subvariables": [
                    "{}subvariables/001/".format(var_url),
                    "{}subvariables/002/".format(var_url),
                    "{}subvariables/003/".format(var_url),
                ],
                "type": "numeric_array",
                "id": "0001",
            },
            "subvar2": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar2",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/002/".format(var_url),
            },
            "subvar3": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar3",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/003/".format(var_url),
            },
            "num_arr_var[subvar3]": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar3",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/003/".format(var_url),
            },
            "subvar1": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar1",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/001/".format(var_url),
            },
            "num_arr_var[subvar1]": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar1",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/001/".format(var_url),
            },
            "num_arr_var[subvar2]": {
                "description": "",
                "parent_id": "0001",
                "alias": "subvar2",
                "is_subvar": True,
                "type": "categorical",
                "id": "{}subvariables/002/".format(var_url),
            },
        }

    def test_get_dataset_variables_categorical_variable(self):
        var_id = "0001"
        var_alias = "my_categorical"
        var_type = "categorical"
        categories = [
            {"name": "mocking", "id": 1},
            {"name": "coding", "id": 2},
        ]

        table_mock = mock.MagicMock(
            metadata={
                var_id: {
                    "id": var_id,
                    "alias": var_alias,
                    "type": var_type,
                    "categories": categories,
                }
            }
        )
        ds = mock.MagicMock()
        ds.follow.return_value = table_mock
        result = get_dataset_variables(ds)
        assert result == {
            var_alias: {
                "alias": var_alias,
                "type": var_type,
                "id": var_id,
                "categories": categories
            }
        }