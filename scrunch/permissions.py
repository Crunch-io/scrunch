
class User:
    _MUTABLE_ATTRIBUTES = {'name', 'email'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, user_resource):
        self.resource = user_resource
        self.url = self.resource.self

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

        # Attribute doesn't exists, must raise an AttributeError
        raise AttributeError('User has no attribute %s' % item)

    def __repr__(self):
        return "<User: email='{}'; id='{}'>".format(self.email, self.id)

    def __str__(self):
        return self.email


class Project:
    _MUTABLE_ATTRIBUTES = {'name', 'description', 'icon'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, project_resource):
        self.resource = project_resource
        self.url = self.resource.self

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

        # Attribute doesn't exists, must raise an AttributeError
        raise AttributeError('Project has no attribute %s' % item)

    def __repr__(self):
        return "<Project: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name




