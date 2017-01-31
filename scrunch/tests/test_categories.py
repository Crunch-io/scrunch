from mock import MagicMock
from unittest import TestCase
from scrunch.datasets import Variable
from scrunch.categories import CategoryList


TEST_CATEGORIES = lambda: [
    {"id": 1, "name": "Female", "missing": False, "numeric_value": None},
    {"id": 2, "name": "Male", "missing": False, "numeric_value": None},
    {"id": -1, "name": "No Data", "missing": True, "numeric_value": None}
]


class EditableMock(MagicMock):
    def edit(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self._edit(**kwargs)


class TestCategories(TestCase):
    def test_instance_is_reused(self):
        resource = EditableMock(
            categories=TEST_CATEGORIES(),
            type='categorical'
        )
        variable = Variable(resource, MagicMock())
        cat_list = variable.categories
        self.assertEqual(id(cat_list), id(variable.categories))
        self.assertTrue(isinstance(cat_list, CategoryList))

    def test_edit_category(self):
        resource = EditableMock(
            categories=TEST_CATEGORIES(),
            type='categorical'
        )
        variable = Variable(resource, MagicMock())
        variable.categories[1].edit(name='Mujer')
        resource._edit.assert_called_with(categories=[
            {'numeric_value': None, 'selected': False, 'id': 1, 'missing': False, 'name': 'Mujer'},
            # These two don't have selected yet because it is reusing the
            # API categories still, only replacing the modified one
            {'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Male'},
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
        ])
        resource.refresh.assert_called_once()
        self.assertEqual(variable.categories[1].name, 'Mujer')

        # Editing Male
        variable.categories[2].edit(name='Hombre')
        resource._edit.assert_called_with(categories=[
            {'numeric_value': None, 'selected': False, 'id': 1, 'missing': False, 'name': 'Mujer'},
            {'numeric_value': None, 'selected': False, 'missing': False, 'id': 2, 'name': 'Hombre'},
            # Same as above, reusing the existing value from API still
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
        ])

        # Try to change the ID
        with self.assertRaises(ValueError) as err:
            variable.categories[2].edit(id=100)
        self.assertEqual(err.exception.message, 'Cannot edit the following attributes: id')

        # Nothing changed
        self.assertEqual(set(variable.categories.keys()), {1, 2, -1})


class TestCategoryList(TestCase):
    def test_reorder(self):
        resource = EditableMock(
            categories=TEST_CATEGORIES(),
            type='categorical'
        )
        variable = Variable(resource, MagicMock())
        variable.categories.order(2, -1, 1)

        # Reordered values
        resource._edit.assert_called_with(categories=[
            {'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Male'},
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'},
            {'numeric_value': None, 'missing': False, 'id': 1, 'name': 'Female'}
        ])
        resource.refresh.assert_called_once()

