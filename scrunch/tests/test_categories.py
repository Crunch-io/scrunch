import pytest
from mock import MagicMock
from unittest import TestCase
from scrunch.datasets import Variable
from scrunch.categories import CategoryList


def TEST_CATEGORIES():
    return [
        {"id": 1, "name": "Female", "missing": False, "numeric_value": None},
        {"id": 2, "name": "Male", "missing": False, "numeric_value": None},
        {"id": -1, "name": "No Data", "missing": True, "numeric_value": None}
    ]


class EditableMock(MagicMock):
    def edit(self, **kwargs):
        self.body.update(kwargs)
        self._edit(**kwargs)


class TestCategories(TestCase):
    def test_instance_is_reused(self):
        resource = EditableMock()
        resource.entity.body = dict(
            categories=TEST_CATEGORIES(),
            type='categorical'
        )
        variable = Variable(resource, MagicMock())
        cat_list = variable.categories
        self.assertTrue(isinstance(cat_list, CategoryList))

    def test_edit_category(self):
        resource = EditableMock()
        resource.entity.body = dict(
            categories=TEST_CATEGORIES(),
            type='categorical'
        )
        variable = Variable(resource, MagicMock())
        variable.categories[1].edit(name='Mujer')
        resource.entity._edit.assert_called_with(categories=[
            {'numeric_value': None, 'selected': False, 'id': 1, 'missing': False, 'name': 'Mujer'},
            # These two don't have selected yet because it is reusing the
            # API categories still, only replacing the modified one
            {'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Male'},
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
        ])
        resource.entity.refresh.assert_called_once()
        self.assertEqual(variable.categories[1].name, 'Mujer')

        # Editing Male
        variable.categories[2].edit(name='Hombre')
        resource.entity._edit.assert_called_with(categories=[
            {'numeric_value': None, 'selected': False, 'id': 1, 'missing': False, 'name': 'Mujer'},
            {'numeric_value': None, 'selected': False, 'missing': False, 'id': 2, 'name': 'Hombre'},
            # Same as above, reusing the existing value from API still
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
        ])

        # Try to change the ID
        with self.assertRaises(AttributeError) as err:
            variable.categories[2].edit(id=100)
        self.assertEqual(
            str(err.exception),
            'Cannot edit the following attributes: id'
        )

        # Nothing changed
        self.assertEqual(set(variable.categories.keys()), {1, 2, -1})

    def test_delete_category(self):
        resource = EditableMock()
        resource.entity.body = dict(
            categories=TEST_CATEGORIES(),
            type='categorical'
        )
        variable = Variable(resource, MagicMock())
        variable.categories[1].delete()
        # Calling edit without the one that we wanted to delete
        resource.entity._edit.assert_called_with(categories=[
            {'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Male'},
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
        ])

    def test_Category_attribute_writes(self):
        resource = EditableMock()
        resource.entity.body = dict(
            categories=TEST_CATEGORIES(),
            type='categorical',
        )
        variable = Variable(resource, MagicMock())

        error_msg = 'use the edit() method for mutating attributes'

        with pytest.raises(AttributeError, message=error_msg):
            variable.categories[1].id = 42
        # nothing has changed
        assert variable.categories[1].id == 1

        with pytest.raises(AttributeError, message=error_msg):
            variable.categories[1].name = 'forbidden'
        # nothing has changed
        assert variable.categories[1].name == 'Female'

        with pytest.raises(AttributeError, message=error_msg):
            variable.categories[1].numeric_value = 42
        # nothing has changed
        assert variable.categories[1].numeric_value is None

        with pytest.raises(AttributeError, message=error_msg):
            variable.categories[1].missing = True
        # nothing has changed
        assert variable.categories[1].missing is False

        with pytest.raises(AttributeError, message=error_msg):
            variable.categories[1].selected = True
        # nothing has changed, default is False
        assert variable.categories[1].selected is False

    def test_edit_derived(self):
        resource = EditableMock()
        resource.entity.body = dict(
            categories=TEST_CATEGORIES(),
            type='categorical',
            derivation={'function': 'derivation_function'}
        )
        variable = Variable(resource, MagicMock())

        error_msg = "Cannot edit categories on derived variables. Re-derive with the appropriate expression"
        with pytest.raises(TypeError, message=error_msg):
            variable.categories[1].edit(name='Mujer')

        # Try again with an empty derivation
        resource = EditableMock()
        resource.entity.body = dict(
            categories=TEST_CATEGORIES(),
            type='categorical',
            derivation={}  # Empty

        )
        variable = Variable(resource, MagicMock())
        variable.categories[1].edit(name='Mujer')
        resource.entity._edit.assert_called_with(categories=[
            {'numeric_value': None, 'selected': False, 'id': 1, 'missing': False, 'name': 'Mujer'},
            {'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Male'},
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'}
        ])


class TestCategoryList(TestCase):
    def test_reorder(self):
        resource = EditableMock()
        resource.entity.body = dict(
            categories=TEST_CATEGORIES(),
            type='categorical'
        )
        variable = Variable(resource, MagicMock())
        variable.categories.order(2, -1, 1)

        # Reordered values
        resource.entity._edit.assert_called_with(categories=[
            {'numeric_value': None, 'missing': False, 'id': 2, 'name': 'Male'},
            {'numeric_value': None, 'missing': True, 'id': -1, 'name': 'No Data'},
            {'numeric_value': None, 'missing': False, 'id': 1, 'name': 'Female'}
        ])
        resource.entity.refresh.assert_called_once()
