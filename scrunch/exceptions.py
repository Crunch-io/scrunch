
class AuthenticationError(Exception):
    """ An exception to signal there was a problem trying to authenticate
    a user.
    """
    pass


class OrderUpdateError(Exception):
    pass


class InvalidPathError(ValueError):
    pass


class InvalidReferenceError(ValueError):
    pass


class InvalidDatasetTypeError(Exception):
    pass


class InvalidVariableTypeError(Exception):
    pass


class InvalidParamError(Exception):
    pass
