from collections import OrderedDict


class Category:
    _MUTABLE_ATTRIBUTES = {'name', 'numeric_value', 'missing', 'selected'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, variable_resource, category):
        self.resource = variable_resource
        self.selected = False  # Default, not always present
        self.fill(category)

    def __repr__(self):
        attrs = self.as_dict()
        return 'Category(%s)' % ', '.join('%s=%s' % c for c in attrs.items())

    def fill(self, kwargs):
        for k, v in kwargs.items():
            if k in self._ENTITY_ATTRIBUTES:
                setattr(self, k, v)

    def as_dict(self):
        return {attr: getattr(self, attr) for attr in self._ENTITY_ATTRIBUTES}

    def edit(self, **kwargs):
        if self.resource.body.get('derivation'):
            raise TypeError("Cannot edit categories on derived variables. Re-derive with the appropriate expression")
        extra_attrs = set(kwargs.keys()) - self._MUTABLE_ATTRIBUTES
        if extra_attrs:
            raise AttributeError("Cannot edit the following attributes: %s" % ', '.join(extra_attrs) )

        self.fill(kwargs)
        categories = [self.as_dict() if cat['id'] == self.id else cat
                      for cat in self.resource.body['categories']]
        self.resource.edit(categories=categories)
        self.resource.refresh()


class CategoryList(OrderedDict):
    def __init__(self, variable_resource):
        self.resource = variable_resource
        categories = [(cat['id'], Category(variable_resource, cat))
                      for cat in variable_resource.body['categories']]
        super(CategoryList, self).__init__(categories)

    def order(self, *new_order):
        categories = sorted(self.resource.body['categories'],
            key=lambda c: new_order.index(c['id']))
        self.resource.edit(categories=categories)
        self.resource.refresh()
