import re


VARIABLE_URL_REGEX = re.compile(
    r"^(http|https):\/\/(.*)\/api\/datasets\/([\w\d]+)\/variables\/([\w\d]+)"
    r"(\/subvariables\/([\w\d]*))?\/?$"
)


def validate_variable_url(url):
    """
    Checks if a given url matches the variable url regex or not.
    """
    return VARIABLE_URL_REGEX.match(url)
