from collections import OrderedDict

from six import PY2

from scrunch.helpers import ReadOnly


class Category(ReadOnly):
    _MUTABLE_ATTRIBUTES = {'name', 'numeric_value', 'missing', 'selected'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, variable_resource, category):
        super(Category, self).__init__(variable_resource)
        self._category = category

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            if item == 'selected':
                # Default is False; not always present
                return self._category.get('selected', False)
            return self._category[item]  # Has to exist

        # Attribute doesn't exists, must raise an AttributeError
        raise AttributeError('Category %s has no attribute %s' % (
            self.category['name'], item))

    def __repr__(self):
        attrs = self.as_dict()
        return 'Category(%s)' % ', '.join('%s=%s' % c for c in attrs.items())

    def as_dict(self, **kwargs):
        dct = {attr: getattr(self, attr) for attr in self._ENTITY_ATTRIBUTES}
        if PY2:
            dct['name'] = dct['name'].encode("ascii", "replace")
        dct.update(**kwargs or {})
        return dct

    def delete(self):
        if self.resource.body.get('derivation'):
            raise TypeError("Cannot delete categories on derived variables. Re-derive with the appropriate expression")

        categories = [cat for cat in self.resource.body['categories']
                      if cat['id'] != self.id]
        self.resource.edit(categories=categories)
        self.resource.refresh()

    def edit(self, **kwargs):
        if self.resource.body.get('derivation'):
            raise TypeError("Cannot edit categories on derived variables. Re-derive with the appropriate expression")
        extra_attrs = set(kwargs.keys()) - self._MUTABLE_ATTRIBUTES
        if extra_attrs:
            raise AttributeError("Cannot edit the following attributes: %s" % ', '.join(extra_attrs))

        categories = [self.as_dict(**kwargs) if cat['id'] == self.id else cat
                      for cat in self.resource.body['categories']]
        self.resource.edit(categories=categories)
        self.resource.refresh()


class CategoryList(OrderedDict):

    @classmethod
    def _from(cls, variable_resource):
        cls.resource = variable_resource
        categories = [(cat['id'], Category(variable_resource, cat))
                      for cat in variable_resource.body['categories']]
        return cls(categories)

    def order(self, *new_order):
        categories = sorted(
            self.resource.body['categories'], key=lambda c: new_order.index(c['id'])
        )
        self.resource.edit(categories=categories)
        self.resource.refresh()
