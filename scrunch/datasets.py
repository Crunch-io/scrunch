import collections
import copy
import datetime
import json
import re
import sys
from warnings import warn
from math import fsum

try:
    import pandas as pd
except ImportError:
    # pandas has not been installed, don't worry!
    # ... unless you have to worry about pandas
    pd = None

import six

import pycrunch
import warnings
from pycrunch import importing
from pycrunch.progress import DefaultProgressTracking
from pycrunch.exporting import export_dataset
from pycrunch.shoji import Entity, TaskProgressTimeoutError, TaskError
from scrunch.categories import CategoryList
from scrunch.exceptions import InvalidParamError, InvalidVariableTypeError
from scrunch.expressions import parse_expr, prettify, process_expr
from scrunch.folders import DatasetFolders
from scrunch.views import DatasetViews
from scrunch.scripts import DatasetScripts, ScriptExecutionError
from scrunch.helpers import (ReadOnly, _validate_category_rules, abs_url,
                             case_expr, download_file, shoji_entity_wrapper,
                             subvar_alias, validate_categories, shoji_catalog_wrapper,
                             get_else_case, else_case_not_selected, SELECTED_ID,
                             NOT_SELECTED_ID, NO_DATA_ID, valid_categorical_date,
                             shoji_view_wrapper)
from scrunch.order import DatasetVariablesOrder, ProjectDatasetsOrder
from scrunch.subentity import Deck, Filter, Multitable
from scrunch.variables import (combinations_from_map, combine_categories_expr,
                               combine_responses_expr, responses_from_map)

from scrunch.connections import LOG, _default_connection, _get_connection


if six.PY2:  # pragma: no cover
    from urlparse import urljoin
else:
    from urllib.parse import urljoin


_MR_TYPE = 'multiple_response'
CATEGORICAL_TYPES = {
    'categorical', 'multiple_response', 'categorical_array',
}
RESOLUTION_TYPES = ['Y', 'Q', 'M', 'W', 'D', 'h', 'm', 's', 'ms']


class SavepointRestore:
    """
    Use this class around a Dataset instance in case you need to restore
    the state when something goes wrong.

    It will create a Savepoint version before starting and delete it
    on success or restore on failure.
    """

    def __init__(self, dataset, description):
        self.dataset = dataset
        self.savepoint = dataset.create_savepoint(description)

    def __enter__(self):
        return self.savepoint

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # Exception! Revert to the savepoint
            self.savepoint.refresh()
            resp = self.savepoint.revert.post({})
            if resp.status_code == 204:
                return   # Empty response, reverted.
            pycrunch.shoji.wait_progress(resp, self.dataset.resource.session)


class NoExclusion:
    """
    Use this context manager to temporarily operate on a dataset ignoring
    the exclusion filter. This will unset and re-set on exit.
    """
    def __init__(self, dataset):
        self.dataset = dataset
        self.exclusion = dataset.get_exclusion()
        empty_exclusion = shoji_entity_wrapper({"expression": {}})
        self.dataset.resource.exclusion.patch(empty_exclusion)

    def __enter__(self):
        return self.dataset

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Always set the exclusion back, exception or not
        self.dataset.exclude(self.exclusion)



def _get_dataset(dataset, connection=None, editor=False, project=None):
    """
    Helper method for specific get_dataset and get_streaming_dataset
    methods. Retrieve a reference to a given dataset (either by name,
    or ID) if it exists and the user has access permissions to it. If
    you have access to the dataset through a project you should do pass
    the project parameter. This method tries to use pycrunch singleton
    connection, environment variables or a crunch.ini config file if the
    optional "connection" parameter isn't provided.

    Also able to change editor while getting the dataset with the
    optional editor parameter.

    Returns a Dataset Entity record if the dataset exists.
    Raises a KeyError if no such dataset exists.

    To get a.BaseDataset from a Project we are building a url and
    making a request through pycrunch.session object, we instead should
    use the /search endpoint from crunch, but currently it's not working
    by id's.
    """
    connection = _default_connection(connection)
    root = connection
    shoji_ds = None
    # search on project if specifed
    if project:
        if isinstance(project, six.string_types):
            project_obj = get_project(project, connection)
            shoji_ds = project_obj.get_dataset(dataset).resource
        else:
            shoji_ds = project.get_dataset(dataset).resource
    else:
        try:
            # search by id on any project
            dataset_url = urljoin(
                root.catalogs.datasets, '{}/'.format(dataset))
            shoji_ds = root.session.get(dataset_url).payload
        except pycrunch.ClientError as e:
            # it is ok to have a 404, it mean that given dataset reference
            # is not an id.
            if e.status_code != 404:
                raise e

        if shoji_ds is None:
            result = root.follow("datasets_by_name", {
                "name": dataset
            })
            if not result.index:
                raise KeyError(
                    "Dataset (name or id: %s) not found in context."
                    % dataset)
            shoji_ds = result.by("name")[dataset].entity

    return shoji_ds, root


# FIXME: to be deprecated in favor of get_streaming_dataset and
# get_mutable_dataset
def get_dataset(dataset, connection=None, editor=False, project=None):
    """
    A simple wrapper of _get_dataset with streaming=False
    """
    shoji_ds, root = _get_dataset(dataset, connection, editor, project)
    ds = Dataset(shoji_ds)
    if editor is True:
        authenticated_url = root.urls["user_url"]
        ds.change_editor(authenticated_url)
    return ds


def get_project(project, connection=None):
    """
    :param project: Crunch project ID or Name
    :param connection: An scrunch session object
    :return: Project class instance
    """
    connection = _default_connection(connection)
    sub_project = None

    if '|' in project:
        project_split = project.split('|')
        project = project_split.pop(0)
        sub_project = '|' + '|'.join(project_split)

    try:
        ret = connection.projects.by('name')[project].entity
    except KeyError:
        try:
            ret = connection.projects.by('id')[project].entity
        except KeyError:
            raise KeyError("Project (name or id: %s) not found." % project)

    _project = Project(ret)

    if sub_project:
        _project = _project.get(sub_project)

    return _project


def get_personal_project(connection=None):
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")
    root = connection
    return Project(root.projects.personal)


def get_user(user, connection=None):
    """
    :param user: Crunch user email address
    :param connection: An scrunch session object
    :return: User class instance
    """
    connection = _default_connection(connection)
    try:
        ret = connection.users.by('email')[user].entity
    except KeyError:
        raise KeyError("User email '%s' not found." % user)
    return User(ret)


def get_team(team, connection=None):
    """
    :param team: Crunch Team Name (crunch only lists teams by Name)
    :param connection: An scrunch session object
    :return: Team class instance
    """
    connection = _default_connection(connection)
    try:
        ret = connection.teams.by('name')[team].entity
    except KeyError:
        raise KeyError("Team name: %s not found." % team)
    return Team(ret)


def create_team(name, connection=None):
    connection = _default_connection(connection)
    shoji_team = connection.teams.create(
        shoji_entity_wrapper({'name': name})).refresh()
    return Team(shoji_team)


def list_geodata(name=None, connection=None):
    """
    :param connection: An scrunch session object
    :return: Dict of geodata objects, keyed by geodata name
    """
    connection = _default_connection(connection)
    return connection.geodata.by('name')


def get_geodata(name=None, connection=None):
    """
    :param name: Geodata name
    :param connection: An scrunch session object
    :return: Geodata object
    """
    try:
        return list_geodata(connection=connection)[name].entity
    except KeyError:
        raise KeyError("Geodata name '%s' not found." % name)


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

    @staticmethod
    def teams():
        """
        Returns a list of Teams where for the current session user
        """
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")
        return list(connection.teams.by('name').keys())


class Members:
    """
    Class for handling collections of members. Adding users,
    removing users, listing users on a resource...
    """

    def __init__(self, resource):
        self.resource = resource

    def list(self, permissions=False):
        """
        :return: A list of members of the Entity as strings. A member
            can be a User or a Team. Returns ['user1@example.com', 'Team A']
        """
        members = {'edit': [], 'view': []} if permissions else []
        for name, member in six.iteritems(self.resource.members.by('name')):
            # members can be users or teams
            user = member.get('email', name)
            if permissions:
                edit = member['permissions'][self._EDIT_ATTRIBUTE]
                group = 'edit' if edit else 'view'
                members[group].append(user)
            else:
                members.append(user)
        return members

    def _validate_member(self, member):
        """
        Validate and instanciate if necessary a member as
        Team or User
        """
        if isinstance(member, User) or isinstance(member, Team):
            return member
        try:
            member = get_user(member)
        except KeyError:
            try:
                member = get_team(member)
            except:
                raise KeyError('Member %s is not a Team nor a User' % member)
        return member

    def remove(self, member):
        """
        :param member: email, User instance, team name or Team instance
        :return: None
        """
        member = self._validate_member(member)
        self.resource.members.patch({member.url: None})

    def add(self, member, edit=False):
        """
        :param member: email, User instance, team name or Team instance
        :return: None
        """
        member = self._validate_member(member)
        self.resource.members.patch({member.url: {
            'permissions': {self._EDIT_ATTRIBUTE: edit}
        }})

    def edit(self, member, edit):
        """
        :param member: email, User instance, team name or Team instance
        Edit a members's permissions on this instance.
        Examples:
            team.members.edit('mathias.bustamante@yougov.com', edit=True)
            project.members.edit('mathias.bustamante@yougov.com', edit=True)
        """
        member = self._validate_member(member)
        self.resource.members.patch({member.url: {
            'permissions': {self._EDIT_ATTRIBUTE: edit}}
        })


class ProjectMembers(Members):
    _EDIT_ATTRIBUTE = 'edit'


class TeamMembers(Members):
    _EDIT_ATTRIBUTE = 'team_admin'


class Team:
    _MUTABLE_ATTRIBUTES = {'name'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, team_resource):
        self.resource = team_resource
        self.url = self.resource.self

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]
        raise AttributeError('Team has no attribute %s' % item)

    def __repr__(self):
        return "<Team: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name

    @property
    def members(self):
        return TeamMembers(self.resource)

    def delete(self):
        return self.resource.delete()


class Project:
    _MUTABLE_ATTRIBUTES = {'name', 'description', 'icon'}
    _IMMUTABLE_ATTRIBUTES = {'id'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES
    LAZY_ATTRIBUTES = {'order'}

    def __init__(self, project_resource):
        self.resource = project_resource
        self.url = self.resource.self
        self._lazy = False

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]

        elif item in self.LAZY_ATTRIBUTES:
            if not self._lazy:
                if self.resource.session.feature_flags['old_projects_order']:
                    datasets = self.resource.datasets
                    self.order = ProjectDatasetsOrder(datasets, datasets.order)
                else:
                    # We detected the new API of nested projects
                    self.order = self  # ;) ;) ;)
                self._lazy = True
            return getattr(self, item)

        raise AttributeError('Project has no attribute %s' % item)

    def __repr__(self):
        return "<Project: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name

    def execute(self, script_body):
        """
        Will run a system script on this project.

        System scripts do not have a return value. If they execute correctly
        they'll finish silently. Otherwise an error will raise.
        """
        # The project execution endpoint is a shoji:view
        payload = shoji_view_wrapper(script_body)
        if "run" in self.resource.views:
            exc_res = self.resource.run  # Backwards compat og API
        else:
            exc_res = self.resource.execute

        try:
            exc_res.post(payload)
        except pycrunch.ClientError as err:
            resolutions = err.args[2]["resolutions"]
            raise ScriptExecutionError(err, resolutions)

    @property
    def members(self):
        return ProjectMembers(self.resource)

    @property
    def users(self):
        """
        TODO: deprecate in favor of members.list property
        """
        LOG.warning("""This method is legacy and will be deprecated
            in future releases. Please make use of project.members.list()
            instead""")  # noqa: E501
        users = []
        return self.members.list()

    def remove_user(self, user):
        """
        TODO: deprecate in favor of members.remove property
        """
        LOG.warning("""This method is legacy and will be deprecated
            in future releases. Please make use of project.members.remove()
            instead""")  # noqa: E501
        self.members.remove(user)

    def add_user(self, user, edit=False):
        """
        TODO: deprecate in favor of members.add property
        """
        LOG.warning("""This method is legacy and will be deprecated
            in future releases. Please make use of project.members.add()
            instead""")  # noqa: E501
        self.members.add(user, edit)

    def edit_user(self, user, edit):
        """
        TODO: deprecate in favor of members.edit property
        """
        LOG.warning("""This method is legacy and will be deprecated
            in future releases. Please make use of project.members.edit()
            instead""")  # noqa: E501
        self.members.edit(user, {'permissions': {'edit': edit}})

    def get_dataset(self, dataset):
        datasets = self.resource.datasets
        try:
            shoji_ds = datasets.by('name')[dataset].entity
        except KeyError:
            try:
                shoji_ds = datasets.by('id')[dataset].entity
            except KeyError:
                raise KeyError(
                    "Dataset (name or id: %s) not found in project." % dataset)
        ds = BaseDataset(shoji_ds)
        return ds

    def create_project(self, name):
        # This should be a method of the Project class
        proj_res = self.resource.create(shoji_entity_wrapper({
            'name': name
        })).refresh()
        return Project(proj_res)

    # Compatibility method to comply with Group API
    create_group = create_project

    @property
    def is_root(self):
        return self.resource.catalogs['project'].endswith('/projects/')

    def get(self, path):
        from scrunch.order import Path, InvalidPathError
        self.resource.refresh()  # Always up to date
        node = self
        for p_name in Path(path).get_parts():
            try:
                node = node.get_child(p_name)
            except KeyError:
                raise InvalidPathError('Project not found %s' % p_name)
        return node

    def __getitem__(self, path):
        return self.get(path)

    def get_child(self, name):
        from scrunch.order import InvalidPathError
        by_name = self.resource.by('name')

        if name in by_name:
            # Found by name, if it's not a folder, return the variable
            tup = by_name[name]
            if tup.type == 'project':
                return Project(tup.entity)
            return self.root.dataset[name]

        raise InvalidPathError('Invalid path: %s' % name)

    @property
    def children(self):
        # Refresh the .resource so it has fresh data in case any child has been
        # deleted.
        self.resource.refresh()
        for child_url in self.resource.graph:
            tup = self.resource.index[child_url]
            if tup['type'] == 'project':
                yield Project(tup.entity)
            elif tup['type'] == 'dataset':
                yield Dataset(tup.entity)

    def delete(self):
        self.resource.delete()

    def rename(self, new_name):
        self.resource.edit(name=new_name)

    def move_here(self, items, **kwargs):
        if not items:
            return
        items = items if isinstance(items, (list, tuple)) else [items]
        position, before, after = [kwargs.get('position'),
                                   kwargs.get('before'), kwargs.get('after')]
        kwargs = {
            'index': {
                item.url: {} for item in items
            }
        }
        if {position, before, after} != {None}:
            # Some of the positional args was not None
            graph = self._position_items(items, position, before, after)
            kwargs['graph'] = graph
        self.resource.patch(shoji_entity_wrapper({}, **kwargs))
        self.resource.refresh()
        for item in items:
            item.resource.refresh()

    def _position_items(self, new_items, position, before, after):
        graph = getattr(self.resource, 'graph', [])
        if before is not None or after is not None:
            # Before and After are strings that map to a Project or Dataset.name
            target = before or after
            index = self.resource.index
            position = [x for x, _u in enumerate(graph) if index[_u]['name'] == target]
            if not position:
                from scrunch.order import InvalidPathError
                raise InvalidPathError("No project with name %s found" % target)
            position = position[0]
            if before is not None:
                position = position if position > 0 else 0
            else:
                max_pos = len(graph)
                position = (position + 1) if position < max_pos else max_pos

        new_items_urls = [c.url for c in new_items]
        if position is not None:
            new_urls = set(new_items_urls)
            children = [_u for _u in graph if _u not in new_urls]
            children[position:0] = new_items_urls
            return children
        return graph + new_items_urls  # Nothing happened, just add

    def place(self, entity, path, position=None, before=None, after=None):
        from scrunch.order import Path, InvalidPathError
        if not Path(path).is_absolute:
            raise InvalidPathError(
                'Invalid path %s: only absolute paths are allowed.' % path
            )
        position = 0 if (before or after) else position
        target = self.get(path)
        target.move_here([entity], position=position, before=before, after=after)

    def reorder(self, items):
        name2tup = self.resource.by('name')
        graph = [
            name2tup[c].entity_url if isinstance(c, six.string_types) else c.url
            for c in items
        ]
        self.resource.patch({
            'element': 'shoji:entity',
            'body': {},
            'index': {},
            'graph': graph
        })
        self.resource.refresh()

    def append(self, *children):
        self.move_here(children)

    def insert(self, *children, **kwargs):
        self.move_here(children, position=kwargs.get('position', 0))

    def move(self, path, position=-1, before=None, after=None):
        from scrunch.order import Path, InvalidPathError
        ppath = Path(path)
        if not ppath.is_absolute:
            raise InvalidPathError(
                'Invalid path %s: only absolute paths are allowed.' % path
            )
        parts = ppath.get_parts()
        top_proj_name, sub_path = parts[0], parts[1:]
        try:
            top_project = self.projects_root().by('name')[top_proj_name].entity
        except KeyError:
            raise InvalidPathError("Invalid target project: %s" % path)

        target = top_project
        for name in sub_path:
            target = target.by('name')[name]
            if not target['type'] == 'project':
                raise InvalidPathError("Invalid target project: %s" % path)
            target = target.entity

        target = Project(target)
        target.move_here([self], position=position, before=before, after=after)

    def projects_root(self):
        # Hack, because we cannot navigate to the projects catalog from a
        # single catalog entity.
        projects_root_url = self.url.rsplit('/', 2)[0] + '/'
        return self.resource.session.get(projects_root_url).payload


class DatasetSettings(dict):

    def __readonly__(self, *args, **kwargs):
        raise RuntimeError('Please use the change_settings() method instead.')

    __setitem__ = __readonly__
    __delitem__ = __readonly__
    pop = __readonly__
    popitem = __readonly__
    clear = __readonly__
    update = __readonly__
    setdefault = __readonly__
    del __readonly__


class DatasetVariablesMixin(collections.Mapping):
    """
    Handles dataset variable iteration in a dict-like way
    """

    def __getitem__(self, item):
        """
        Returns a Variable() instance, `item` can be either a variable alias,
        name or URL
        """
        # Check if the attribute corresponds to a variable alias
        variable = self._catalog.by('alias').get(item)
        if variable is None:  # Not found by alias
            variable = self._catalog.by('name').get(item)
            if variable is None:  # Not found by name
                variable = self._catalog.index.get(item)
                if variable is None:  # Not found by URL
                    # Variable doesn't exists, must raise a ValueError
                    raise ValueError(
                        'Entity %s has no (sub)variable with a name or alias %s'
                        % (self.name, item))
        # make sure we pass the parent dataset to subvariables
        if isinstance(self, Variable):
            return Variable(variable, self.dataset)
        return Variable(variable, self)

    def _set_catalog(self):
        self._catalog = self.resource.variables

    def _reload_variables(self):
        """
        Helper that takes care of updating self._vars on init and
        whenever the dataset adds a variable
        """
        self._set_catalog()
        self._vars = self._catalog.index.items()
        self._order = None

    @property
    def order(self):
        """
        The `order` property, which provides a high-level API for
        manipulating the "Hierarchical Order" structure of a Dataset.
        """

        if "hier" not in self._catalog.orders:
            raise TypeError("This dataset does not expose a hierarchical order. Use .folders")

        if self._order is None:
            order = self._catalog.hier
            self._order = DatasetVariablesOrder(self._catalog, order)
        return self._order

    def _var_create_reload_return(self, payload):
        """
        helper function for POSTing to variables, reload
        the catalog of variables and return newly created var
        """
        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return an instance of Variable
        return self[new_var['self']]

    def __iter__(self):
        for var in self._vars:
            yield var

    def __len__(self):
        return len(self._vars)

    def itervalues(self):
        for _, var_tuple in self._vars:
            yield Variable(var_tuple, self)

    def iterkeys(self):
        """
        Yield variable alias, since they are unique
        """
        for var in self._vars:
            yield var[1].alias

    def variable_names(self):
        """
        Simply return a list of all variable names in the Dataset
        """
        return [var[1].name for var in self._vars]

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return zip(self.iterkeys(), self.itervalues())


class DefaultWeight:
    pass


class BaseDataset(ReadOnly, DatasetVariablesMixin):
    """
    A pycrunch.shoji.Entity wrapper that provides basic dataset methods.
    """

    _MUTABLE_ATTRIBUTES = {'name', 'notes', 'description', 'is_published',
                           'archived', 'end_date', 'start_date', 'streaming'}
    _IMMUTABLE_ATTRIBUTES = {'id', 'creation_time', 'modification_time',
                             'size'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES
    _EDITABLE_SETTINGS = {'viewers_can_export', 'viewers_can_change_weight',
                          'viewers_can_share', 'dashboard_deck',
                          'variable_folders'}

    def __init__(self, resource):
        """
        :param resource: Points to a pycrunch Shoji Entity for a dataset.
        """
        super(BaseDataset, self).__init__(resource)
        self._settings = None
        self._order = None
        # since we no longer have an __init__ on DatasetVariablesMixin because
        # of the multiple inheritance, we just initiate self._vars here
        self._reload_variables()
        self.folders = DatasetFolders(self)
        self.scripts = DatasetScripts(self.resource)

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist
        # Default behaviour
        return object.__getattribute__(self, item)

    def __repr__(self):
        return "<{}: name='{}'; id='{}'>".format(
            self.__class__.__name__, self.name, self.id)

    def __str__(self):
        return self.name

    @property
    def editor(self):
        try:
            return User(self.resource.follow('editor_url'))
        except pycrunch.lemonpy.ClientError:
            return self.resource.body.current_editor

    @editor.setter
    def editor(self, _):
        # Protect the `editor` from external modifications.
        raise TypeError('Unsupported operation on the editor property')

    def change_editor(self, user):
        """
        Change the current editor of the Crunch dataset.

        :param user:
            The email or a User instance of the user who should
            be set as the new current editor of the given dataset.
        """
        if isinstance(user, User):
            user_url = user.url
        elif "@" in user:
            # Is this an email? Then discover the URL for that user
            user = get_user(user)
            user_url = user.url
        else:
            # Otherwise, assume the provided argument is a URL
            user_url = user

        payload = shoji_entity_wrapper({'current_editor': user_url})
        self.resource.patch(payload)
        self.resource.refresh()

    def make_mutable(self):
        from scrunch.mutable_dataset import MutableDataset
        return MutableDataset(self.resource)

    def make_streaming(self):
        from scrunch.streaming_dataset import StreamingDataset
        self.edit(streaming='streaming')
        return StreamingDataset(self.resource)

    @property
    def project(self):
        return Project(self.resource.project)

    @property
    def owner(self):
        warn("Access Dataset.project instead", DeprecationWarning)
        owner_url = self.resource.body.owner
        try:
            if '/users/' in owner_url:
                return User(self.resource.follow('owner_url'))
            else:
                return Project(self.resource.follow('owner_url'))
        except pycrunch.lemonpy.ClientError:
            return owner_url

    @owner.setter
    def owner(self, _):
        # Protect `owner` from external modifications.
        raise TypeError(
            'Unsupported operation on the owner property'
        )

    def change_owner(self, user=None, project=None):
        """
        :param user: email or User object
        :param project: id, name or Project object
        :return:
        """
        warn("Use Dataset.move() to move datasets between projects", DeprecationWarning)
        if user and project:
            raise AttributeError(
                "Must provide user or project. Not both"
            )

        if user:
            warn("Changing owner to users is deprecated. Move to projects", DeprecationWarning)
            if not isinstance(user, User):
                user = get_user(user)
            owner_url = user.url
            self.resource.patch({'owner': owner_url})
            self.resource.refresh()
        elif project:
            if not isinstance(project, Project):
                project = get_project(project)
            self.move(project)
        else:
            raise AttributeError("Can't set owner")

    def move(self, project):
        if not isinstance(project, Project):
            project = get_project(project)

        project.move_here([self])  # This performs .resource.refresh()

    @property
    def settings(self):
        if self._settings is None:
            self._load_settings()
        return self._settings

    @settings.setter
    def settings(self, _):
        # Protect the `settings` property from external modifications.
        raise TypeError('Unsupported operation on the settings property')

    @property
    def filters(self):
        _filters = {}
        for f in self.resource.filters.index.values():
            filter_inst = Filter(f)
            _filters[filter_inst.name] = filter_inst
        return _filters

    @filters.setter
    def filters(self, _):
        # Protect the `filters` property from external modifications.
        raise TypeError('Use add_filter method to add filters')

    @property
    def decks(self):
        _decks = {}
        for d in self.resource.decks.index.values():
            deck_inst = Deck(d)
            _decks[deck_inst.id] = deck_inst
        return _decks

    @decks.setter
    def decks(self, _):
        # Protect the `decks` property from external modifications.
        raise TypeError('Use add_deck method to add a new deck')

    @property
    def multitables(self):
        _multitables = {}
        for mt in self.resource.multitables.index.values():
            mt_instance = Multitable(mt, self)
            _multitables[mt_instance.name] = mt_instance
        return _multitables

    @multitables.setter
    def multitables(self, _):
        # Protect the `multitables` property from direct modifications
        raise TypeError('Use the `create_multitable` method to add one')

    @property
    def crunchboxes(self):
        from scrunch.crunchboxes import CrunchBox
        _crunchboxes = []
        for shoji_tuple in self.resource.boxdata.index.values():
            _crunchboxes.append(CrunchBox(shoji_tuple, self))
        return _crunchboxes

    @crunchboxes.setter
    def crunchboxes(self, _):
        # Protect the `crunchboxes` property from direct modifications
        raise TypeError('Use the `create_crunchbox` method to add one')

    def _load_settings(self):
        settings = self.resource.session.get(
            self.resource.fragments.settings).payload
        self._settings = DatasetSettings(
            (_name, _value) for _name, _value in settings.body.items()
        )
        return self._settings

    def change_settings(self, **kwargs):
        incoming_settings = set(kwargs.keys())
        invalid_settings = incoming_settings.difference(
            self._EDITABLE_SETTINGS)
        if invalid_settings:
            raise ValueError(
                'Invalid or read-only settings: %s'
                % ','.join(list(invalid_settings))
            )

        if 'dashboard_deck' in kwargs:
            ddeck = kwargs['dashboard_deck']
            if isinstance(ddeck, Deck):
                kwargs['dashboard_deck'] = ddeck.resource.self

        settings_payload = {
            setting: kwargs[setting] for setting in incoming_settings
        }
        if settings_payload:
            self.resource.session.patch(
                self.resource.fragments.settings,
                json.dumps(settings_payload),
                headers={'Content-Type': 'application/json'}
            )
            self._settings = None
        # After changing settings, reload folders that depend on it
        self.resource.refresh()
        self.folders = DatasetFolders(self)

    def edit(self, **kwargs):
        """
        Edit main Dataset attributes
        """
        for key in kwargs:
            if key not in self._MUTABLE_ATTRIBUTES:
                raise AttributeError(
                    "Can't edit attibute %s of dataset %s"
                    % (key, self.name))
            if key in ['start_date', 'end_date'] and \
                    (isinstance(kwargs[key], datetime.date) or
                     isinstance(kwargs[key], datetime.datetime)
                     ):
                kwargs[key] = kwargs[key].isoformat()

        return self.resource.edit(**kwargs)

    def add_user(self, user, edit=False):
        """
        :param user: email or User instance, or list/tuple of same
        :param edit: is the user an editor in the Dataset
        :return: None
        """
        if isinstance(user, (list, tuple)):
            for _user in user:
                self.add_user(_user, edit)
            return
        # always use the email, to assure an invite
        if isinstance(user, User):
            user = user.email

        payload = {
            'send_notification': True,
            'message': "",
            user: {
                'dataset_permissions': {
                    'view': True,
                    'edit': edit,
                },
            },
            'url_base':
                self.resource.self.split('api')[0]
                + 'password/change/${token}/',
            'dataset_url':
                self.resource.self.replace('/api/datasets/', '/dataset/'),
        }
        self.resource.permissions.patch(payload)

    def create_fill_values(self, variables, name, alias, description=''):
        """
        This function is similar to create_single_categorical in the sense
        that the output is a 1D variable.

        Will create a derived variable using a combination of Crunch's `fill`
        and `case` functions, to create a new variable using the values from
        the specified variables according to each expression.

            dataset.create_fill_values([
                {"case": "pop_pref == 1", "variable": "coke_freq"},
                {"case": "pop_pref == 2", "variable": "pepsi_freq"},
                # {"case": "else", "variable": "any_freq"},
                {"case": "else", "missing": True, "name": "Not Asked", "id": 99},
            ], alias="pop_freq", name="Pop frequency")

        The `else` case can be either a variable or a default category.
            * In the case of variable it should have the shape:
                {"case": "else", "variable": "<alias>"}
            * In the case of a default category, it should indicate:
                {"case": "else", "name": "Cat Name", "missing": <bool>, "id": <int cat code>}

        :param variables: list of dictionaries with an `variable` and `case`
        :param name: Name of the new variable
        :param alias: Alias of the new variable
        :param description: Description of the new variable
        :return:
        """
        if not hasattr(self.resource, 'variables'):
            self.resource.refresh()

        # Pluck `else` case out.
        else_case = [c for c in variables if c["case"] == "else"]
        else_case = else_case[0] if else_case else {}
        variables = [c for c in variables if c["case"] != "else"]

        if "variable" in else_case and "name" in else_case:
            raise ValueError("Else case can be either variable or category not both")

        aliases = {c["variable"] for c in variables}
        vars_by_alias = self.resource.variables.by("alias")
        types = {vars_by_alias[al]["type"] for al in aliases}
        if types != {"categorical"}:
            raise ValueError("All variables must be of type `categorical`")

        cat_ids = list(range(1, len(variables) + 1))
        args = [{
            "column": cat_ids,
            "type": {
                "class": "categorical",
                "ordinal": False,
                "categories": [
                    {"id": c, "name": str(c), "missing": False, "numeric_value": None}
                    for c in cat_ids
                ]
            }
        }]
        exprs = [parse_expr(c["case"]) for c in variables]
        exprs = process_expr(exprs, self.resource)
        args.extend(exprs)

        if "name" in else_case:
            # We are in the else_case of a category. Add there the extra default
            args[0]["column"].append(else_case["id"])
            args[0]["type"]["categories"].append({
                "name": else_case["name"],
                "missing": else_case.get("missing", False),
                "id": else_case["id"],
                "numeric_value": else_case.get("numeric_value", None),
            })

        expr = {"function": "case", "args": args}
        fill_map = {str(cid): {"variable": vars_by_alias[v["variable"]]["id"]}
                    for cid, v in zip(cat_ids, variables)}

        if "variable" in else_case:
            # We are in the case of a default fill, replace the -1 with the new
            # variable
            fill_map["-1"] = {"variable": vars_by_alias[else_case["variable"]]["id"]}

        fill_expr = {
            "function": "fill",
            "args": [
                expr,
                {"map": fill_map}
            ]
        }
        payload = shoji_entity_wrapper({
            "alias": alias,
            "name": name,
            "description": description,
            "derivation": fill_expr
        })
        return self._var_create_reload_return(payload)

    def create_single_response(self, categories, name, alias, description='',
        missing=True, notes=''):
        """
        Creates a categorical variable deriving from other variables.
        Uses Crunch's `case` function.
        """
        cases = []
        # keep a copy of categories because we are gonna mutate it later
        categories_copy = [copy.copy(c) for c in categories]
        for cat in categories:
            case = cat.pop('case')
            case = get_else_case(case, categories_copy)
            cases.append(case)
            # append a default numeric_value if not found
            if 'numeric_value' not in cat:
                cat['numeric_value'] = None

        if not hasattr(self.resource, 'variables'):
            self.resource.refresh()

        args = [{
            'column': [c['id'] for c in categories],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': categories
                }
            }
        }]

        for cat in args[0]['type']['value']['categories']:
            cat.setdefault('missing', False)

        if missing:
            args[0]['column'].append(-1)
            args[0]['type']['value']['categories'].append(dict(
                id=-1,
                name='No Data',
                numeric_value=None,
                missing=True))

        more_args = []
        for case in cases:
            more_args.append(parse_expr(case))

        more_args = process_expr(more_args, self.resource)

        expr = dict(function='case', args=args + more_args)

        payload = shoji_entity_wrapper(dict(
            alias=alias,
            name=name,
            expr=expr,
            description=description,
            notes=notes))
        return self._var_create_reload_return(payload)

    def rollup(self, variable_alias, name, alias, resolution, description='',
        notes=''):
        """
        Rolls the source datetime variable into a new derived categorical variable.
        Available resolutions are: [Y, Q, M, W, D, h, m, s, ms]
        :variable_alias: source datetime variable alias to rollup from
        :name: name of the new derived variable
        :alias: alias for the new derived variable
        :resolution: one of [Y, Q, M, W, D, h, m, s, ms]
        """
        assert self[variable_alias].type == 'datetime', \
            'rollup() is only allowed for datetime variable types'

        self._validate_vartypes(self[variable_alias].type, resolution)

        expr = {
            'function': 'rollup',
            'args': [
                {
                    'variable': self[variable_alias].url
                },
                {
                    'value': resolution
                }
            ]
        }

        payload = shoji_entity_wrapper(dict(
            alias=alias,
            name=name,
            expr=expr,
            description=description,
            notes=notes))

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return the variable instance
        return self[new_var['body']['alias']]

    def derive_multiple_response(self, categories, subvariables, name, alias,
        description='', notes='', uniform_basis=False):
        """
        This is the generic approach to create_multiple_response but this
        allows the definition of any set of categories and rules (expressions)
        for it's subvariables to fit these defined categories.
        :param categories: a list of category dictionary:
            categories=[{id: 1, name: 'Yes', 'selected': True} ...]
        :param subvariables: a list of dictionary definind rules and attributes for each subvariable:
            subvariables=[
                {
                    'id': 1,
                    'name': 'Subvar 1',
                    'cases': {
                        1: 'var_1 < 20', 2: 'var_1 == 20', 3: 'var_1 == 30', 4: 'var_1 > 30'
                    }
                }
            ]
        :param name: Name of the variable to create
        :param alias: Alias of the variable to create
        :param description: Description of the variable to create
        :param notes: Notes of the variable to create

        Note: It's important that each subvariable defines the same number of cases for every category.
        Suvariable alias will be derived from the alias to the variable + subvariable id to keep
        compliance with other methods in Scrunch
        """
        # validate categories and fill defaults
        categories = validate_categories(categories)
        # validate that every subvar defines rules for all categories
        for subvar in subvariables:
            _validate_category_rules(categories, subvar['cases'])

        responses_map = collections.OrderedDict()
        for subvar in subvariables:
            _cases = []
            for case in subvar['cases'].values():
                if isinstance(case, six.string_types):
                    _case = process_expr(parse_expr(case), self.resource)
                    _cases.append(_case)

            resp_id = '%04d' % subvar['id']
            responses_map[resp_id] = case_expr(
                _cases,
                name=subvar['name'],
                alias='%s_%d' % (alias, subvar['id']),
                categories=categories
            )

        payload = shoji_entity_wrapper({
            'name': name,
            'alias': alias,
            'description': description,
            'notes': notes,
            'uniform_basis': uniform_basis,
            'derivation': {
                'function': 'array',
                'args': [{
                    'function': 'select',
                    'args': [
                        {'map': responses_map},
                        {'value': list(responses_map.keys())}
                    ]
                }]
            }
        })
        return self._var_create_reload_return(payload)

    def create_multiple_response(self, responses, name, alias, description='',
        notes=''):
        """
        Creates a Multiple response (array) of only 2 categories, selected and not selected.
        """
        responses_map = collections.OrderedDict()

        for resp in responses:
            case = resp['case']
            case = get_else_case(case, responses)
            if isinstance(case, six.string_types):
                case = process_expr(parse_expr(case), self.resource)

            resp_id = '%04d' % resp['id']
            responses_map[resp_id] = case_expr(
                [case,],
                name=resp['name'],
                alias='%s_%d' % (alias, resp['id'])
            )

        payload = shoji_entity_wrapper({
            'name': name,
            'alias': alias,
            'description': description,
            'notes': notes,
            'derivation': {
                'function': 'array',
                'args': [{
                    'function': 'select',
                    'args': [
                        {'map': responses_map},
                        {'value': list(responses_map.keys())}
                    ]
                }]
            }
        })
        return self._var_create_reload_return(payload)

    def bind_categorical_array(self, name, alias, subvariables, description='',
        notes=''):
        """
        Creates a new categorical_array where subvariables is a
        subset of categorical variables already existing in the DS.

        Creates a derived variable.

        :param: name: Name of the new variable.
        :param: alias: Alias of the new variable
        :param: description: A description of the new variable
        :param: notes: Notes to attach to the new variable
        :param: subvariables: a list of dictionary elements:
            subvariables = [
                {'id': 1, 'alias': var1},
                {'id': 2, 'alias': var2}
            ]
        """

        # creates numeric ids if 'id' not present in subvariables list

        for i, elem in enumerate(subvariables, start=1):
            if 'id' not in elem:
                elem.update({'id': i})

        payload = shoji_entity_wrapper({
            'name': name,
            'alias': alias,
            'description': description,
            'notes': notes,
            'derivation': {
                'function': 'array',
                'args': [{
                    'function': 'select',
                    'args': [{
                        'map': {v['id']: {'variable': self[v['alias']].url} for v in subvariables}
                    }]
                }]
            }
        })
        return self._var_create_reload_return(payload)

    def create_numeric(self, alias, name, derivation, description='', notes=''):
        """
        Used to create new numeric variables using Crunch's derived
        expressions
        """
        expr = process_expr(parse_expr(derivation), self.resource)

        if not hasattr(self.resource, 'variables'):
            self.resource.refresh()

        payload = shoji_entity_wrapper(dict(
            alias=alias,
            name=name,
            derivation=expr,
            description=description,
            notes=notes
        ))
        return self._var_create_reload_return(payload)

    def create_categorical(self, categories, alias, name, multiple, description='',
        notes='', missing_case=None, uniform_basis=False):
        """
        Used to create new categorical variables using Crunchs's `case`
        function

        Will create either categorical variables or multiple response depending
        on the `multiple` parameter.
        (1) If the variable to create is a categorical, then arguments need to look like:
            categories=[
                {'id': 1, 'name': 'Millennial', 'case': 'age_var < 25'},
                {'id': 2, 'name': 'Gen X', 'case': 'age_var > 25'},
                {'id': 3, 'name': 'Other', 'case': 'else'}  --> optional
            ],
            multiple=False

        (2) If the variable needed is a Multiple Response with two default categories:
            1: Selected
            2: Not Selected

            Then categories argument categories should look like:
            categories=[
                {'id': 1, 'name': 'variable 1', 'case': 'var_1 == 1'},
                {'id': 2, 'name': 'variable 2', 'case': 'var_2 == 2'},
                {'id': 3, 'name': 'variable_3', 'case': 'var_3 == 3'},
                {'id': 4, 'name': 'Other', 'case': 'else'}  --> optional
            ],
            multiple=True

            * Default categories for every subvaraible are:
                {'id': 1, 'name': 'Selected', 'missing': False, 'selected': True}
                {'id': 2, 'name': 'Not selected', 'missing': False, 'selected': False}

        (3) If the variable to create is a Multiple Response with three default categories:
            1: Selected
            2: Not Selected
            3: No Data

            Then we need to declare the extra Missing case, which can be done in one of the
            follwing 2 ways:
                (A) Every/some subvariable declare it's own missing_case individually in the
                    `missing_case` element
                categories: [
                    {
                        'case': 'var_1 == 1',
                        'name': 'subvar_1',
                        'id': 1,
                        'missing_case': 'var_1 == 3'
                    },
                    {
                        'case': 'var_1 == 2',
                        'name': 'subvar_2',
                        'id': 2,
                        'missing_case': 'var_1 == 4'
                    },
                    {
                        'case': 'var_1 == 3',
                        'name': 'subvar_3',
                        'id': 3,
                    },
                    {
                        'case': 'else',
                        'name': 'Other',
                        'id': 4,  --> optional
                    }],
                multiple=True
                (B) If the missing_case is constant across all subvariables, then the argument
                    `missing_case` can be passed as argument to this function:
                categories: [
                    {
                        'case': 'var_1 == 1',
                        'name': 'subvar_1',
                        'id': 1,
                    },
                    {
                        'case': 'var_1 == 2',
                        'name': 'subvar_2',
                        'id': 2,
                    },
                    {
                        'case': 'var_1 == 3',
                        'name': 'subvar_3',
                        'id': 3,
                    },
                    {
                        'case': 'else',
                        'name': 'Other',
                        'id': 4,  --> optional
                    }],
                multiple=True,
                missing_case='missing(var_1)'
        """
        cats_have_missing = any(['missing_case' in c.keys() for c in categories])

        # Initially validate that we dont have `missing_case` argument and `missing_case`
        # in the categories list
        if missing_case and cats_have_missing:
            raise ValueError(
                'missing_case as an argument and as element of "categories" is not allowed'
            )
        # First we append the missing_case to every subvariable and let the
        # generic case deal with it
        if missing_case:
            cats_have_missing = True
            for cat in categories:
                cat['missing_case'] = missing_case

        # In the case of MR and all cases declare a 'missing_case'
        if multiple and cats_have_missing:
            _categories = [
                {'id': SELECTED_ID, 'name': 'Selected', 'selected': True},
                {'id': NOT_SELECTED_ID, 'name': 'Not Selected'},
                {'id': NO_DATA_ID, 'name': 'No Data', 'missing': True}
            ]
            _subvariables = []
            for sv in categories:
                data = {
                    'id': sv['id'],
                    'name': sv['name']
                }

                # build special expressions for 'else' case if exist
                else_not_selected = else_case_not_selected(sv['case'], categories, sv.get('missing_case'))
                sv['case'] = get_else_case(sv['case'], categories)

                if 'missing_case' in sv:
                    selected_case = '({}) and not ({})'.format(sv['case'], sv['missing_case'])
                    not_selected_case = 'not {}'.format(selected_case)
                    if else_not_selected:
                        not_selected_case = else_not_selected
                    data.update({
                        'cases': {
                            SELECTED_ID: selected_case,
                            NOT_SELECTED_ID: not_selected_case,
                            NO_DATA_ID: sv['missing_case']
                        }
                    })
                else:
                    data.update({
                        'cases': {
                            SELECTED_ID: sv['case'],
                            NOT_SELECTED_ID: 'not ({})'.format(sv['case']),
                        }
                    })
                _subvariables.append(data)

            return self.derive_multiple_response(categories=_categories,
                subvariables=_subvariables, name=name, alias=alias,
                description=description, notes=notes, uniform_basis=uniform_basis)

        elif multiple:
            return self.create_multiple_response(
                categories, alias=alias, name=name, description=description,
                notes=notes)
        else:
            return self.create_single_response(
                categories, alias=alias, name=name, description=description,
                notes=notes)

    def _validate_vartypes(self, var_type, resolution=None, subvariables=None,
        categories=None):
        if var_type not in ('text', 'numeric', 'categorical', 'datetime',
                            'multiple_response', 'categorical_array'):
            raise InvalidVariableTypeError

        if var_type == 'datetime' and resolution not in RESOLUTION_TYPES:
            raise InvalidParamError(
                'Include a valid resolution parameter when creating \
                datetime variables. %s' % RESOLUTION_TYPES)

        array_types = ('multiple_response', 'categorical_array')
        if var_type in array_types and not isinstance(subvariables, list):
            raise InvalidParamError(
                'Include subvariables when creating %s variables' % var_type)

    def create_variable(self, var_type, name, alias=None, description='',
        resolution=None, subvariables=None, categories=None, values=None):
        """
        A variable can be of type: text, numeric, categorical, datetime,
        multiple_response or categorical_array.

        Type datetime: must include resolution ("Y", "M", "D", "h", "m",
            "s", and "ms")
        Types multiple_response and categorical array: must include
        subvariables.

        :param: name: Name for the variable.
        :param: alias: Alias for the new variable or auto-created in
            Crunch if None.
        :param: description: Description for the variable.
        :param: resolution: A string with one of ("Y", "M", "D", "h", "m",
            "s", and "ms").
        :param: subvariables: A list defining the subvariables for
            multiple_response and categorical_array variable types in the form:
            subvariables = [
                {'name' 'Subvariable 1'},
                {'name' 'Subvariable 2'}
            ]
        :param: categories: List of categories in the form:
            categories = [
                {'name': 'ManU', 'id': 1, 'numeric_value': 1, 'missing': False},
                {'name': 'Chelsea', 'id': 2, 'numeric_value': 2, 'missing': False},
                {'name': 'Totthenham', 'id': 3, 'numeric_value': 3, 'missing': False}
            ]

            If vay_type is multiple_response, categories is optional and will
            default to:
            categories = [
                {'name': 'Not selected', 'id': 2, 'numeric_value': 2, 'missing': False},
                {'name': 'Selected', 'id': 1, 'numeric_value': 1, 'missing': False, 'selected': True},
            ]
            Note: You need to include 'selected': True, or a multiple_response,
                will be converted to a categorical_array
        :param: values: a list of values to populate the variable with.
            values = [1,4,5,2,1,3,1]
        """
        self._validate_vartypes(var_type, resolution, subvariables, categories)
        payload = {
            'type': var_type,
            'name': name,
            'description': description,
        }
        if alias:
            payload['alias'] = alias
        if resolution:
            payload['resolution'] = resolution
        if var_type == 'multiple_response' and categories is None:
            payload['categories'] = [
                {'name': 'Not selected', 'id': NOT_SELECTED_ID, 'numeric_value': 2, 'missing': False},
                {'name': 'Selected', 'id': SELECTED_ID, 'numeric_value': 1, 'missing': False, 'selected': True},
            ]
        if categories:
            payload['categories'] = categories
        if subvariables:
            payload['subreferences'] = []
            for item in subvariables:
                subrefs = {'name': item['name']}
                if item.get('alias'):
                    subrefs['alias'] = item['alias']
                payload['subreferences'].append(subrefs)
        if values:
            payload['values'] = values

        self._var_create_reload_return(shoji_entity_wrapper(payload))

    def copy_variable(self, variable, name, alias, derived=None):
        _subvar_alias = re.compile(r'.+_(\d+)$')

        def subrefs(_variable, _alias):
            # In the case of MR variables, we want the copies' subvariables
            # to have their aliases in the same pattern and order that the
            # parent's are, that is `parent_alias_#`.
            _subreferences = []
            for _, subvar in _variable:
                sv_alias = subvar['alias']
                match = _subvar_alias.match(sv_alias)
                if match:  # Does this var have the subvar pattern?
                    suffix = int(match.groups()[0], 10)  # Keep the position
                    sv_alias = subvar_alias(_alias, suffix)

                _subreferences.append({
                    'name': subvar['name'],
                    'alias': sv_alias
                })
            return _subreferences

        if variable.derived:
            # We are dealing with a derived variable, we want the derivation
            # to be executed again instead of doing a `copy_variable`
            derivation = abs_url(variable.resource.body['derivation'],
                                 variable.resource.self)
            derivation.pop('references', None)
            payload = shoji_entity_wrapper({
                'name': name,
                'alias': alias,
                'derivation': derivation})

            if variable.type == _MR_TYPE:
                # We are re-executing a multiple_response derivation.
                # We need to update the complex `array` function expression
                # to contain the new suffixed aliases. Given that the map is
                # unordered, we have to iterated and find a name match.
                _ob = payload['body']['derivation']['args'][0]['args'][0]
                subvars = _ob['map']
                subreferences = subrefs(variable, alias)
                for subref in subreferences:
                    for subvar_pos in subvars:
                        subvar = subvars[subvar_pos]
                        if subvar['references']['name'] == subref['name']:
                            subvar['references']['alias'] = subref['alias']
                            break
        else:
            payload = shoji_entity_wrapper({
                'name': name,
                'alias': alias,
                'derivation': {
                    'function': 'copy_variable',
                    'args': [{
                        'variable': variable.resource.self
                    }]
                }
            })

        if derived is False or derived:
            payload['body']['derived'] = derived

        return self._var_create_reload_return(payload)

    def combine_categories(self, variable, map, categories, missing=None,
        default=None, name='', alias='', description=''):
        if not alias or not name:
            raise ValueError("Name and alias are required")
        if variable.type in _MR_TYPE:
            return self.combine_multiple_response(
                variable, map, categories, name=name,
                alias=alias, description=description)
        else:
            return self.combine_categorical(
                variable, map, categories, missing, default,
                name=name, alias=alias, description=description)

    def combine_categorical(self, variable, map, categories=None, missing=None,
        default=None, name='', alias='', description=''):
        """
        Create a new variable in the given dataset that is a recode
        of an existing variable
            map={
                1: (1, 2),
                2: 3,
                3: (4, 5)
            },
            default=9,
            missing=[-1, 9],
            categories={
                1: "low",
                2: "medium",
                3: "high",
                9: "no answer"
            },
            missing=9
        """
        if isinstance(variable, six.string_types):
            variable = self[variable]

        # TODO: Implement `default` parameter in Crunch API
        combinations = combinations_from_map(
            map, categories or {}, missing or [])
        payload = shoji_entity_wrapper({
            'name': name,
            'alias': alias,
            'description': description,
            'derivation': combine_categories_expr(
                variable.resource.self, combinations)
        })
        return self._var_create_reload_return(payload)

    def combine_multiple_response(self, variable, map, categories=None, default=None,
        name='', alias='', description=''):
        """
        Creates a new variable in the given dataset that combines existing
        responses into new categorized ones
            map={
                1: 1,
                2: [2, 3, 4]
            },
            categories={
                1: "online",
                2: "notonline"
            }
        """
        if isinstance(variable, six.string_types):
            parent_alias = variable
            variable = self[variable]
        else:
            parent_alias = variable.alias

        # TODO: Implement `default` parameter in Crunch API
        responses = responses_from_map(variable, map, categories or {}, alias,
                                       parent_alias)
        payload = shoji_entity_wrapper({
            'name': name,
            'alias': alias,
            'description': description,
            'derivation': combine_responses_expr(
                variable.resource.self, responses)
        })
        return self._var_create_reload_return(payload)

    def cast_summary(self, variable, cast_type):
        """
        Returns a summary of the given variable when trying
        to cast it's type to "cast_type".
        :return: A json payload indicating success on the cast intention or
        an possible error on it. It will raise an error
        """
        try:
            resp = self.resource.session.get(
                self[variable].resource.views.cast,
                params={'cast_as': cast_type}
            )
        except pycrunch.lemonpy.ClientError as e:
            return 'Impossible to cast var "%s" to type "%s". Error: %s' % (
                variable, cast_type, e)
        return resp.content

    def cast(self, variable, cast_type):
        """
        Casts a variable to numeric, text or categorical.
        @param variable: variable alias in the dataset to cast.
        @param cast_type: one of ['numeric', 'text', 'categorical']
        :return: the casted variable or an error
        """
        allowed_types = 'numeric', 'text', 'categorical'
        assert cast_type in allowed_types, "Cast type not allowed"
        payload = {'cast_as': cast_type}
        # try casting the variable in place
        self.resource.session.post(
            self[variable].resource.views.cast,
            data=json.dumps(payload))
        # make sure to update the dataset variables with the casted one
        self._reload_variables()
        return self[variable]

    def create_savepoint(self, description):
        """
        Creates a savepoint on the dataset.

        :param description:
            The description that should be given to the new savepoint. This
            function will not let you create a new savepoint with the same
            description as any other savepoint.
        """
        if len(self.resource.savepoints.index) > 0:
            if description in self.savepoint_attributes('description'):
                raise KeyError(
                    "A checkpoint with the description '{}' already"
                    " exists.".format(description)
                )

        sp = shoji_entity_wrapper({'description': description})
        return self.resource.savepoints.create(sp)

    def load_savepoint(self, description=None):
        """
        Load a savepoint on the dataset.

        :param description: default=None
            The description that identifies which savepoint to be loaded.
            When loading a savepoint, all savepoints that were saved after
            the loaded savepoint will be destroyed permanently.
        """
        if description is None:
            description = 'initial import'
        elif description not in self.savepoint_attributes('description'):
            raise KeyError(
                "No checkpoint with the description '{}'"
                " exists.".format(description)
            )

        sp = self.resource.savepoints.by('description').get(description)
        self.resource.session.post(sp.revert)
        self._reload_variables()

    def savepoint_attributes(self, attrib):
        """
        Return list of attributes from the given dataset's savepoints.

        :param attrib:
            The attribute to be returned for each savepoint in the given
            dataset. Available attributes are:
                'creation_time'
                'description'
                'last_update'
                'revert'
                'user_name'
                'version'
        """
        svpoints = self.resource.savepoints
        if len(svpoints.index) != 0:
            attribs = [
                cp[attrib]
                for url, cp in six.iteritems(svpoints.index)
            ]
            return attribs
        return []

    def create_crunchbox(self, title='', header='', footer='', notes='',
        filters=None, variables=None, force=False, min_base_size=None,
        weight=DefaultWeight, palette=None):
        """
        create a new boxdata entity for a CrunchBox.

        NOTE: new boxdata is only created when there is a new combination of
        where and filter data.

        Args:
            title       (str): Human friendly identifier
            header      (str): header information for the CrunchBox
            footer      (str): footer information for the CrunchBox
            notes       (str): Other information relevent for this CrunchBox
            weight      (str): URL of the weight to apply, None for unweighted
            filters    (list): list of filter names or `Filter` instances
            variables  (list): list of variable aliases or `Variable` instances
                               If `None` all variables will be included.
            min_base_size (int): min sample size to display values in graph
            palette    (dict): dict of colors as documented at docs.crunch.io
                i.e.
                {
                    "brand": ["#111111", "#222222", "#333333"],
                    "static_colors": ["#444444", "#555555", "#666666"],
                    "base": ["#777777", "#888888", "#999999"],
                    "category_lookup": {
                        "category name": "#aaaaaa",
                        "another category:": "bbbbbb"
                }

        Returns:
            CrunchBox (instance)
        """

        if filters:
            if not isinstance(filters, list):
                raise TypeError('`filters` argument must be of type `list`')

            # ensure we only have `Filter` instances
            filters = [
                f if isinstance(f, Filter) else self.filters[f]
                for f in filters
            ]

            if any(not f.is_public
                    for f in filters):
                raise ValueError('filters need to be public')

            filters = [
                {'filter': f.resource.self}
                for f in filters
            ]

        if variables:
            if not isinstance(variables, list):
                raise TypeError('`variables` argument must be of type `list`')

            # ensure we only have `Variable` Tuples
            # NOTE: if we want to check if variables are public we would have
            # to use Variable instances instead of their Tuple representation.
            # This would cause additional GET's
            variables = [
                var.shoji_tuple if isinstance(var, Variable)
                else self.resource.variables.by('alias')[var]
                for var in variables
            ]

            variables = dict(
                function='select',
                args=[
                    {'map': {
                        v.id: {'variable': v.entity_url}
                        for v in variables
                    }}
                ])

        # use weight from preferences, remove in #158676482
        if weight is DefaultWeight:
            preferences = self.resource.session.get(
                self.resource.fragments.preferences)
            weight = preferences.payload.body.weight or None

        if not title:
            title = 'CrunchBox for {}'.format(str(self))

        payload = shoji_entity_wrapper(dict(
            weight=weight,
            where=variables,
            filters=filters,
            force=force,
            title=title,
            notes=notes,
            header=header,
            footer=footer)
        )

        if min_base_size:
            payload['body'].setdefault('display_settings', {}).update(
                dict(minBaseSize=dict(value=min_base_size)))
        if palette:
            payload['body'].setdefault('display_settings', {}).update(
                dict(palette=palette))

        # create the boxdata
        self.resource.boxdata.create(payload)

        # NOTE: the entity from the response is a bit different compared to
        # others, i.e. no id, no delete method, different entity_url...
        # For now, return the shoji_tuple from the index
        from scrunch.crunchboxes import CrunchBox
        for shoji_tuple in self.resource.boxdata.index.values():
            if shoji_tuple.metadata.title == title:
                return CrunchBox(shoji_tuple, self)

    def delete_crunchbox(self, **kwargs):
        """ deletes crunchboxes on matching kwargs """
        match = False
        for key in kwargs:
            if match:
                break
            for crunchbox in self.crunchboxes:
                attr = getattr(crunchbox, key, None)
                if attr and attr == kwargs[key]:
                    crunchbox.remove()
                    match = True
                    break

    def forks_dataframe(self):
        """
        Return a dataframe summarizing the forks on the dataset.

        :returns _forks : pandas.DataFrame
            A DataFrame representation of all attributes from all forks
            on the given dataset.
        """
        if pd is None:
            raise ImportError(
                "Pandas is not installed, please install it in your "
                "environment to use this function."
            )

        if len(self.resource.forks.index) == 0:
            return None
        else:
            _forks = pd.DataFrame(
                [fk for url, fk in six.iteritems(self.resource.forks.index)]
            )
            _forks = _forks[[
                'name',
                'description',
                'is_published',
                'owner_name',
                'current_editor_name',
                'creation_time',
                'modification_time',
                'id'
            ]]
            _forks['creation_time'] = pd.to_datetime(_forks['creation_time'])
            _forks['modification_time'] = pd.to_datetime(
                _forks['modification_time'])
            _forks.sort_values(by=['creation_time'], inplace=True)

            return _forks

    def export(self, path, format='csv', filter=None, variables=None,
        hidden=False, options=None, metadata_path=None, timeout=None):
        """
        Downloads a dataset as CSV or as SPSS to the given path. This
        includes hidden variables.

        Dataset viewers can't download hidden variables so we default
        to False. Dataset editors will need to add hidden=True if they
        need this feature.

        By default, categories in CSV exports are provided as id's.
        """
        valid_options = ['use_category_ids', 'prefix_subvariables',
                         'var_label_field', 'missing_values']

        # Only CSV and SPSS exports are currently supported.
        if format not in ('csv', 'spss'):
            raise ValueError(
                'Invalid format %s. Allowed formats are: "csv" and "spss".'
                % format
            )

        if format == 'csv':
            # Default options for CSV exports.
            export_options = {'use_category_ids': True}
        else:
            # Default options for SPSS exports.
            export_options = {
                'prefix_subvariables': False,
                'var_label_field': 'description'
            }

        # Validate the user-provided export options.
        options = options or {}
        if not isinstance(options, dict):
            raise ValueError(
                'The options argument must be a dictionary.'
            )

        for k in options.keys():
            if k not in valid_options:
                raise ValueError(
                    'Invalid options for format "%s": %s.'
                    % (format, ','.join(k))
                )
        if 'var_label_field' in options \
                and not options['var_label_field'] in ('name', 'description'):
            raise ValueError(
                'The "var_label_field" export option must be either "name" '
                'or "description".'
            )

        # All good. Update the export options with the user-provided values.
        export_options.update(options)

        # the payload should include all hidden variables by default
        payload = {'options': export_options}

        # Option for exporting metadata as json
        if metadata_path is not None:
            metadata = self.resource.table['metadata']
            if variables is not None:
                if sys.version_info >= (3, 0):
                    metadata = {
                        key: value
                        for key, value in metadata.items()
                        if value['alias'] in variables
                    }
                else:
                    metadata = {
                        key: value
                        for key, value in metadata.iteritems()
                        if value['alias'] in variables
                    }
            with open(metadata_path, 'w+') as f:
                json.dump(metadata, f, sort_keys=True)

        # add filter to rows if passed
        if filter:
            if isinstance(filter, Filter):
                payload['filter'] = {'filter': filter.resource.self}
            else:
                payload['filter'] = process_expr(
                    parse_expr(filter), self.resource)

        # convert variable list to crunch identifiers
        if variables and isinstance(variables, list):
            id_vars = []
            for var in variables:
                id_vars.append(self[var].url)
            if len(id_vars) != len(variables):
                LOG.debug(
                    "Variables passed: %s Variables detected: %s"
                    % (variables, id_vars)
                )
                raise AttributeError("At least a variable was not found")
            # Now build the payload with selected variables
            payload['where'] = {
                'function': 'select',
                'args': [{
                    'map': {
                        x: {'variable': x} for x in id_vars
                    }
                }]
            }
        # hidden is mutually exclusive with
        # variables to include in the download
        if hidden and not variables:
            if not self.resource.body.permissions.edit:
                raise AttributeError(
                    "Only Dataset editors can export hidden variables")
            payload['where'] = {
                'function': 'select',
                'args': [{
                    'map': {
                        x: {'variable': x}
                        for x in self.resource.variables.index.keys()
                    }
                }]
            }

        progress_tracker = pycrunch.progress.DefaultProgressTracking(timeout)
        url = export_dataset(
            dataset=self.resource,
            options=payload,
            format=format,
            progress_tracker=progress_tracker
        )
        download_file(url, path)

    def exclude(self, expr=None):
        """
        Given a dataset object, apply an exclusion filter to it (defined as an
        expression string).

        If the `expr` parameter is None, an empty expression object is sent as
        part of the PATCH request, which effectively removes the exclusion
        filter (if any).

        Exclusion filters express logic that defines a set of rows that should
        be dropped from the dataset. The rows aren't permanently deleted---you
        can recover them at any time by removing the exclusion filter---but
        they are omitted from all views and calculations, as if they had been
        deleted.

        Note that exclusion filters work opposite from how "normal" filters
        work. That is, a regular filter expression defines the subset of rows
        to operate on: it says "keep these rows." An exclusion filter defines
        which rows to omit. Applying a filter expression as a query filter will
        have the opposite effect if applied as an exclusion. Indeed, applying
        it as both query filter and exclusion at the same time will result in 0
        rows.
        """
        if isinstance(expr, six.string_types):
            expr_obj = parse_expr(expr)
            # cause we need URLs
            expr_obj = process_expr(expr_obj, self.resource)
        elif expr is None:
            expr_obj = {}
        else:
            expr_obj = expr

        return self.resource.session.patch(
            self.resource.fragments.exclusion,
            data=json.dumps(dict(expression=expr_obj))
        )

    def get_exclusion(self):
        exclusion = self.resource.exclusion
        if 'body' not in exclusion:
            return None
        expr = exclusion['body'].get('expression')
        return prettify(expr, self) if expr else None

    def add_filter(self, name, expr, public=False):
        payload = shoji_entity_wrapper(dict(
            name=name,
            expression=process_expr(parse_expr(expr), self.resource),
            is_public=public))
        new_filter = self.resource.filters.create(payload)
        return self.filters[new_filter.body['name']]

    def add_deck(self, name, description="", public=False):
        payload = shoji_entity_wrapper(dict(
            name=name,
            description=description,
            is_public=public))
        new_deck = self.resource.decks.create(payload)
        return self.decks[new_deck.self.split('/')[-2]]

    def fork(self, description=None, name=None, is_published=False,
        preserve_owner=True, **kwargs):
        """
        Create a fork of ds and add virgin savepoint.

        :param description: str, default=None
            If given, the description to be applied to the fork. If not
            given the description will be copied from ds.
        :param name: str, default=None
            If given, the name to be applied to the fork. If not given a
            default name will be created which numbers the fork based on
            how many other forks there are on ds.
        :param is_published: bool, default=False
            If True, the fork will be visible to viewers of ds. If False it
            will only be viewable to editors of ds.
        :param preserve_owner: bool, default=True
            If True, the owner of the fork will be the same as the parent
            dataset otherwise the owner will be the current user in the
            session and the Dataset will be set under `Persona Project`

        :returns _fork: scrunch.datasets.BaseDataset
        """
        from scrunch.mutable_dataset import MutableDataset
        nforks = len(self.resource.forks.index)
        if name is None:
            if six.PY2:
                name = "FORK #{} of {}".format(
                    nforks + 1,
                    self.resource.body.name.encode("ascii", "ignore"))
            else:
                name = "FORK #{} of {}".format(
                    nforks + 1,
                    self.resource.body.name)
        if description is None:
            description = self.resource.body.description

        body = dict(
            name=name,
            description=description,
            is_published=is_published,
            **kwargs
        )

        if preserve_owner:
            body['owner'] = self.resource.body.owner
        # not returning a dataset
        payload = shoji_entity_wrapper(body)
        _fork = self.resource.forks.create(payload).refresh()
        # return a MutableDataset always
        fork_ds = MutableDataset(_fork)  # Fork has same editor as current user
        return fork_ds

    def replace_values(self, variables, filter=None, literal_subvar=False):
        """
        :param variables: dictionary, {var_alias: value, var2_alias: value}.
            Alows subvariable alias as well
        :param filter: string, an Scrunch expression, i.e; 'var_alias > 1'
        """
        payload = {
            'command': 'update',
            'variables': {},
        }

        for alias, val in variables.items():
            if isinstance(val, list):
                if literal_subvar:
                    payload['variables'][alias] = {'column': val}
                else:
                    payload['variables'][self[alias].id] = {'column': val}
            else:
                if literal_subvar:
                    payload['variables'][alias] = {'value': val}
                else:
                    payload['variables'][self[alias].id] = {'value': val}
        if filter:
            payload['filter'] = process_expr(parse_expr(filter), self.resource)

        # Remove query parameters from table url
        table = self.resource.table
        table.self = table.self[:table.self.find('?')]

        resp = self.resource.table.post(json.dumps(payload))
        if resp.status_code == 204:
            LOG.info('Dataset Updated')
            return
        pycrunch.shoji.wait_progress(resp, self.resource.session)
        return resp

    def backfill_from_csv(self, aliases, pk_alias, csv_fh, rows_filter=None, timeout=None):
        """

        :param aliases: List of strings for the aliases present in the CSV file
        :param pk_alias: Alias of the column to use as PK (Must be the same in
            the dataset and present in the CSV file)
        :param csv_fh: File handler for the CSV file
        :param rows_expr: String expression that corresponds for the rows
            we want to backfil "pk > 100 and pk < 150"
        :return:
        """

        MAX_FILE_SIZE = 150 * 2 ** 20  # 150MB

        file_size = len(csv_fh.read())
        if file_size >= MAX_FILE_SIZE:
            raise ValueError("Max CSV allowed size is currently 150MB")
        csv_fh.seek(0)

        if rows_filter is not None:
            rows_filter = process_expr(parse_expr(rows_filter), self.resource)
        back_filler = BackfillFromCSV(self, pk_alias, aliases, rows_filter, timeout)
        back_filler.execute(csv_fh)

    def replace_from_csv(self, filename, chunksize=1000):
        """
        Given a csv file in the format:
        id, var1_alias, var2_alias
        1,  14,         15

        where the first column is the Dataset PK

        Replace the values of the matching id, for the given variables
        in the Dataset using the /stream endpoint:

        [{id: 1, var1_alias: 14, var2_alias: 15}, ...]
        """
        warnings.warn(
            "This method is deprecated. Use Dataset.backfill_from_csv",
            PendingDeprecationWarning)
        streaming_state = self.resource.body.get('streaming', 'no')
        ds = self
        if streaming_state != 'streaming':
            ds = self.make_streaming()
        importer = pycrunch.importing.Importer()
        df_chunks = pd.read_csv(
            filename,
            header=0,
            chunksize=chunksize
        )
        for chunk in df_chunks:
            # This is a trick to get rid of np.int64, which is not
            # json serializable
            stream = chunk.to_json(orient='records')
            stream = json.loads(stream)
            # trap the timeout and allow it to finish
            try:
                importer.stream_rows(self.resource, stream)
                # We force the row push to instantly see any errors in the data
                # and to allow changing to streaming status back to it's previous
                # state
                ds.push_rows(chunksize)
            except TaskProgressTimeoutError as exc:
                exc.entity.wait_progress(exc.response)
        if streaming_state != 'streaming':
            ds.edit(streaming=streaming_state)

    def merge(self, fork_id=None, autorollback=True):
        """
        :param fork_id: str or int
            can either be the fork id, name or its number as string or int

        :param autorollback: bool, default=True
            if True the original dataset is rolled back to the previous state
            in case of a merge conflict.
            if False the dataset and fork are beeing left 'dirty'
        """
        if isinstance(fork_id, int) or (
                isinstance(fork_id, six.string_types) and
                fork_id.isdigit()):
            fork_id = "FORK #{} of {}".format(fork_id, self.resource.body.name)

        elif fork_id is None:
            raise ValueError('fork id, name or number missing')

        fork_index = self.resource.forks.index

        forks = [f for f in fork_index
                 if fork_index[f].get('name') == fork_id or
                 fork_index[f].get('id') == fork_id]
        if len(forks) == 1:
            fork_url = forks[0]
        else:
            raise ValueError(
                "Couldn't find a (unique) fork. "
                "Please try again using its id")

        body = dict(
            dataset=fork_url,
            autorollback=autorollback)

        resp = self.resource.session.post(
            self.resource.actions.self,
            data=json.dumps(shoji_entity_wrapper(body)))
        if resp.status_code == 204:
            LOG.info("Dataset merged")
            return
        elif resp.status_code == 202:
            if 'location' in resp.headers:
                LOG.info("Dataset merge in progress, see %s" %
                         resp.headers['location'])
            else:
                LOG.info("Dataset merge in progress, but no location header. "
                         "Content %s" % resp.content)
        return resp

    def delete_forks(self):
        """
        Deletes all the forks on the dataset. CANNOT BE UNDONE!
        """
        for fork in six.itervalues(self.resource.forks.index):
            fork.entity.delete()

    def create_multitable(self, name, template, is_public=False):
        """
        template: List of dictionaries with the following keys
        {"query": <query>, "transform": <transform>|optional}.
        A query is a variable or a function on a variable:
        {"query": bin(birthyr)}

        If transform is specified it must have the form
        {
            "query": var_x,
            "transform": {"categories": [
                {
                    "missing": false,  --> default: False
                    "hide": true,      --> default: True
                    "id": 1,
                    "name": "not asked"
                },
                {
                    "missing": false,
                    "hide": true,
                    "id": 4,
                    "name": "skipped"
                }
            ]}
        }
        """
        # build template payload
        parsed_template = []

        for q in template:
            processed = False
            # sometimes q is not a dict but simply a string, convert
            # it to a dict
            if isinstance(q, str) or isinstance(q, Variable):
                q = {"query": q}
            as_json = {}
            # the special case of q being a multiple_response variable alias,
            # we need to build a different payload

            if q['query'] in self.keys():
                # this means is a variable in this dataset
                var_alias = q['query']
                var_url = self[var_alias].resource.self
                multi_types = 'multiple_response', 'categorical_array'
                if self[var_alias].type in multi_types:
                    as_json['query'] = [
                        {
                            'each': var_url
                        },
                        {
                            'function': 'as_selected',
                            'args': [
                                {
                                    'variable': var_url
                                }
                            ]
                        }
                    ]
                    processed = True
                else:
                    as_json['query'] = [{'variable': var_url}]
                    processed = True

            elif isinstance(q['query'], Variable):
                var_url = q['query'].resource.self
                as_json['query'] = [{'variable': var_url}]
                processed = True

            if not processed:
                parsed_q = process_expr(parse_expr(q['query']), self.resource)
                # wrap the query in a list of one dict element
                as_json['query'] = [parsed_q]
            if 'transform' in q.keys():
                as_json['transform'] = q['transform']

            parsed_template.append(as_json)

        payload = shoji_entity_wrapper(dict(
            name=name,
            is_public=is_public,
            template=parsed_template))

        new_multi = self.resource.multitables.create(payload)
        return self.multitables[new_multi.body['name']]

    def import_multitable(self, name, multi):
        """
        Copies a multitable from another Dataset into this one:
        As described at http://docs.crunch.io/#post176
        :name: Name of the new multitable
        :multi: Multitable instance to clone into this Dataset
        """
        payload = shoji_entity_wrapper(dict(
            name=name,
            multitable=multi.resource.self))
        self.resource.multitables.create(payload)
        return self.multitables[name]

    def set_weight(self, variables=None):
        """
        :param: variables: List of variable aliases to set as weight.

        As default Crunch behaviour, the list will be overwritten on
        every request. Use a None if you need to unset all weights.
        """
        if isinstance(variables, list):
            graph = [self[v].url for v in variables]
        if variables is None:
            graph = []
        payload = {'graph': graph}
        return self.resource.variables.weights.patch(json.dumps(payload))

    @property
    def weights(self):
        weight_urls = self.resource.variables.weights.graph
        return [self.resource.variables.index[weight_alias].alias
            for weight_alias in weight_urls]

    def remove_weight(self, variables):
        """
        :param: variables: List of variable aliases or
        sting of variable alias to remove from weights
        """
        if not isinstance(variables, six.string_types) and \
                not isinstance(variables, list):
            raise TypeError("variable must be a string or a list")

        weights = self.weights
        if isinstance(variables, list):
            for var in variables:
                if var in weights:
                    weights.remove(var)
                else:
                    raise NameError("%s was not found in weights" % var)
        else:
            if variables in weights:
                weights.remove(variables)
            else:
                raise NameError("%s was not found in weights" % variables)

        graph = [self[v].url for v in weights]
        payload = {'graph': graph}
        return self.resource.variables.weights.patch(json.dumps(payload))

    def drop_rows(self, filter):
        """
        :param: filter: An scrunch filter expression that matches rows to drop
        """
        filters = process_expr(parse_expr(filter), self.resource)
        payload = {
            'command': 'delete',
            'filter': filters,
        }
        self.resource.table.post(json.dumps(payload))

    @property
    def size(self):
        """
        Exposes the dataset's size object as a property of the dataset instance
        """
        return self.resource.body.size

    def derive_weight(self, targets, alias, name, description=''):
        """
        Derives a new variable to be used as raked weight.
        https://docs.crunch.io/feature-guide/feature-deriving.html?highlight=rake#weights
        :param targets: A list of dictionaries where each dictionary is keyed by a variable alias
            and the items are the actual targets to that particular variable.
        An example:
            targets = [
                {
                    'gender' : {
                        1: .45,
                        2: .55
                    }
                },
                {
                    'locality': {
                        1: .10,
                        2: .15,
                        3: .20,
                        4: .25,
                        5: .30
                    }
                }
            ]
        """
        _targets = []
        for target in targets:
            for key, val in target.items():
                if fsum(val.values()) != 1.0:
                    raise ValueError('Weights for target {} need to add up to 1.0'.format(key))
                _targets.append({
                    'variable': self[key].id,
                    'targets': list(map(list, val.items()))
                })

        payload = shoji_entity_wrapper({
            'name': name,
            'alias': alias,
            'description': description,
            'derivation': {
                'function': 'rake',
                'args': _targets
            }
        })
        return self._var_create_reload_return(payload)

    @property
    def is_view(self):
        return self.resource.body["view_of"] is not None

    @property
    def views(self):
        if not self.is_view:
            # This means that this is not a view, it is a regular dataset
            return DatasetViews(self.resource)
        else:
            raise TypeError("Cannot access views of views")


class Dataset(BaseDataset):

    _BASE_MUTABLE_ATTRIBUTES = {'streaming'}

    def __init__(self, resource):
        LOG.warning("""Dataset is deprecated, instead use now
            mutable_datasets.MutableDataset or streaming_dataset.StreamingDataset
            with it's corresponding get_mutable_dataset and get_streaming_dataset
            methods""")  # noqa: E501
        super(Dataset, self).__init__(resource)
        self._MUTABLE_ATTRIBUTES = self._BASE_MUTABLE_ATTRIBUTES | self._BASE_MUTABLE_ATTRIBUTES


class DatasetSubvariablesMixin(DatasetVariablesMixin):

    def _reload_variables(self):
        """
        Helper that takes care of updating self._vars on init and
        whenever the dataset adds a variable
        """
        self._vars = []
        self._catalog = {}
        if getattr(self.resource, 'subvariables', None):
            self._catalog = self.resource.subvariables
            self._vars = self._catalog.index.items()

    def __iter__(self):
        if getattr(self.resource, 'subvariables', None):
            for var_url in self.subvariables:
                yield (var_url, dict(self._vars)[var_url])


class MissingRules(dict):
    """
    Handles variables missing rules in a dict fashion.
    del var.missing_rules['skipped']  --> deletes a missing rule
    var.missing_rules['not asked'] = 999  --> adds a missing rule
    """

    def __init__(self, resource, *args):
        self.resource = resource
        # remove the nested key: {'value': value} to mimic
        # pythonic dict behaviour
        data = {}
        for k, v in args[0].items():
            data[k] = v['value']
        dict.__init__(self, data)

    def __setitem__(self, key, value):
        data = {key: {'value': value}}
        for k, v in self.items():
            # wrap value in a {'value': value} for crunch
            data[k] = {'value': v}
            if key == k:
                data[k]['value'] = value
        # send the json to the missing_rules endpoint
        result = self.resource.session.put(
            self.resource.fragments.missing_rules,
            json.dumps({'rules': data}))
        assert result.status_code == 204
        super(MissingRules, self).__setitem__(key, value)

    def __delitem__(self, key):
        assert self[key]
        data = {}
        for k, v in self.items():
            # wrap value in a {'value': value} for crunch
            data[k] = {'value': v}
        del data[key]
        result = self.resource.session.put(
            self.resource.fragments.missing_rules,
            json.dumps({'rules': data}))
        assert result.status_code == 204
        super(MissingRules, self).__delitem__(key)

    def clear(self):
        self.resource.session.put(
            self.resource.fragments.missing_rules,
            json.dumps({'rules': {}}))
        super(MissingRules, self).clear()


class Variable(ReadOnly, DatasetSubvariablesMixin):
    """
    A pycrunch.shoji.Entity wrapper that provides variable-specific methods.
    DatasetSubvariablesMixin provides for subvariable interactions.
    """
    _MUTABLE_ATTRIBUTES = {'name', 'description', 'uniform_basis',
                           'view', 'notes', 'format', 'derived'}
    _IMMUTABLE_ATTRIBUTES = {'id', 'alias', 'type', 'discarded'}
    # We won't expose owner and private
    # categories in immutable. IMO it should be handled separately
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES
    _OVERRIDDEN_ATTRIBUTES = {'categories'}

    def __init__(self, var_tuple, dataset):
        """
        :param var_tuple: A Shoji Tuple for a dataset variable
        :param dataset: a Dataset object instance
        """
        self.shoji_tuple = var_tuple
        self.is_instance = False
        self._resource = None
        self.url = var_tuple.entity_url
        self.dataset = dataset
        self._reload_variables()
        if self._is_alias_mutable():
            self._MUTABLE_ATTRIBUTES.add('alias')
            self._IMMUTABLE_ATTRIBUTES.discard('alias')

    def _is_alias_mutable(self):
        if self.dataset.resource.body.get('streaming') == 'no' and not self.derived:
            return True
        return False

    @property
    def is_subvar(self):
        return 'subvariables' in self.url

    @property
    def resource(self):
        if not self.is_instance:
            self._resource = self.shoji_tuple.entity
            self.is_instance = True
        return self._resource

    def __getattr__(self, item):
        # don't access self.resource unless necessary
        if hasattr(self.shoji_tuple, item):
            return self.shoji_tuple[item]
        if item in self._ENTITY_ATTRIBUTES - self._OVERRIDDEN_ATTRIBUTES:
            try:
                return self.resource.body[item]  # Has to exist
            except KeyError:
                raise AttributeError(
                    "Variable does not have attribute %s" % item)
        return super(Variable, self).__getattribute__(item)

    def edit(self, **kwargs):
        for key in kwargs:
            if key not in self._MUTABLE_ATTRIBUTES:
                raise AttributeError(
                    "Can't edit attribute %s of variable %s"
                    % (key, self.name))
        self.dataset._reload_variables()
        return self.resource.edit(**kwargs)

    def __repr__(self):
        return "<Variable: name='{}'; id='{}'>".format(self.name, self.id)

    def __str__(self):
        return self.name

    @property
    def categories(self):
        if self.resource.body['type'] not in CATEGORICAL_TYPES:
            raise TypeError(
                "Variable of type %s do not have categories"
                % self.resource.body.type)
        return CategoryList._from(self.resource)

    def delete(self):
        self.resource.delete()
        self.dataset._reload_variables()

    def hide(self):
        self.resource.edit(discarded=True)

    def unhide(self):
        self.resource.edit(discarded=False)

    def integrate(self):
        if self.derived:
            self.resource.edit(derived=False)
            self.dataset._reload_variables()

    def add_category(self, id, name, numeric_value, missing=False, date=None, before_id=False):
        if self.resource.body['type'] not in CATEGORICAL_TYPES:
            raise TypeError(
                "Variable of type %s do not have categories"
                % self.resource.body.type)

        if self.resource.body.get('derivation'):
            raise TypeError("Cannot add categories on derived variables. Re-derive with the appropriate expression")

        categories = self.resource.body['categories']
        category_data = {
            'id': id,
            'missing': missing,
            'name': name,
            'numeric_value': numeric_value,
        }
        if date is not None:
            if not isinstance(date, six.string_types):
                raise ValueError("Date must be a string")
            if not valid_categorical_date(date):
                raise ValueError("Date must conform to Y-m-d format")
            category_data["date"] = date

        if before_id:
            # only accept int type
            assert isinstance(before_id, int)

            # see if id exist
            try:
                self.categories[before_id]
            except:
                raise AttributeError('before_id not found: {}'.format(before_id))

            new_categories = []
            for category in categories:
                if category['id'] == before_id:
                    new_categories.append(category_data)
                new_categories.append(category)
            categories = new_categories
        else:
            categories.append(category_data)

        resp = self.resource.edit(categories=categories)
        self._reload_variables()
        return resp

    def edit_categorical(self, categories, rules):
        # validate rules and categories are same size
        _validate_category_rules(categories, rules)
        args = [{
            'column': [c['id'] for c in categories],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': categories}}}]
        # build the expression
        more_args = []
        for rule in rules:
            more_args.append(parse_expr(rule))
        # get dataset and build the expression
        more_args = process_expr(more_args, self.dataset)
        # epression value building
        expr = dict(function='case', args=args + more_args)
        payload = shoji_entity_wrapper(dict(expr=expr))
        # patch the variable with the new payload
        resp = self.resource.patch(payload)
        self._reload_variables()
        return resp

    def edit_derived(self, variable, mapper):
        raise NotImplementedError("Use edit_combination")

    def move(self, path, position=-1, before=None, after=None):
        self.dataset.order.place(self, path, position=position,
                                 before=before, after=after)

    def move_to_folder(self, path, position=None, after=None, before=None):
        target = self.dataset.folders.get(path)
        target.move_here(self, position=position, after=after, before=before)

    def unbind(self):
        """ Unbinds all subvariables from the current Array type
        variable. Works only for non-derived material variables
        """
        payload = json.dumps(shoji_entity_wrapper({'unbind': []}))
        resp = self.resource.post(payload)
        return resp

    @property
    def missing_rules(self):
        if self.resource.body['type'] in CATEGORICAL_TYPES:
            raise TypeError(
                "Variable of type %s do not have missing rules"
                % self.resource.body.type)

        result = self.resource.session.get(
            self.resource.fragments.missing_rules)
        assert result.status_code == 200
        return MissingRules(self.resource, result.json()['body']['rules'])

    def set_missing_rules(self, rules):
        """
        Updates the variable's missing rules.

        :param rules: a dictionary of rules for missing values, missing reason
                as key, rule as value. The rule can be one of:
        Sample:

            missing_rules = {
                "not asked": 9999,
                "skipped": 9998}

            ds['varname'].set_missing_rules(missing_rules)
        """
        if self.resource.body['type'] in CATEGORICAL_TYPES:
            raise TypeError(
                "Variable of type %s do not have missing rules"
                % self.resource.body.type)

        data = {}
        for k, v in rules.items():
            # wrap value in a {'value': value} for crunch
            data[k] = {'value': v}
        result = self.resource.session.put(
            self.resource.fragments.missing_rules,
            json.dumps({'rules': data})
        )
        assert result.status_code == 204

    def set_geodata_view(self, geodata, feature_key):
        """
        Enables geodata view for the variable.

        :param geodata: url, name or Entity of the geodatum to use
        :param feature_key: key defined for each Feature in the
                            geojson/topojson that matches the relevant
                            field on the variable
        """

        # we need the geodata url
        if isinstance(geodata, six.string_types):
            is_url = (
                geodata.startswith('http://') or geodata.startswith('https://')
            )

            if not is_url:
                # is a name, get the url
                geodata = get_geodata(geodata)

        if isinstance(geodata, Entity):
            geodata = geodata.self

        self._resource.patch({
            'view': {
                'geodata': [
                    {
                        'geodatum': geodata,
                        'feature_key': feature_key
                    }
                ]
            }
        })

        self._resource.refresh()

    def unset_geodata_view(self):
        """
        Unsets the geodata view for the variable
        """

        view = self.view

        if 'geodata' in view:
            view['geodata'] = []
            self._resource.patch({'view': view})
            self._resource.refresh()

    def replace_values(self, value, filter=None):
        """
        Proxy method to parent Dataset replace_values focused
        especifically for this variable or subvariable instance.

        For subvariables, we need to pass a weird syntax to the
        update command:
            {'variable_id.subvariable_id': value}
        """
        if self.is_subvar:
            subvar_reference = '{}.{}'.format(self.resource.variable.body.id, self.id)
            return self.dataset.replace_values(
                {subvar_reference: value}, filter=filter, literal_subvar=True
            )
        return self.dataset.replace_values({self.alias: value}, filter=filter)

    def reorder_subvariables(self, subvariables):
        """
        This method reorders the variable subvariables list.
        :param: subvariables: a list of subvariable aliases
        :return: a new Variable instance. Note that since we
        are moving things around in the API, we need to fetch
        the resource again. An example of how to use this:

        new_var = old_var.reorder_subvariables(['alias1', 'alias2'])
        """
        # verify there is no repeated aliases
        assert len(set(subvariables)) == len(subvariables), \
            'Repeated aliases found in subvariables: {}'.format(subvariables)
        # verify there is no missing subvariables
        assert sorted(list(self.keys())) == sorted(subvariables), \
            'Missing subvariables for this Variable. Existing: {}. Given: {}'.format(
                list(self.keys()), subvariables)

        reordered_urls = [self[sv].url for sv in subvariables]
        self.resource.patch(json.dumps({'subvariables': reordered_urls}))
        self.dataset._reload_variables()
        return self.dataset[self.alias]

    def cat_names_to_ids(self, cat_list):
        cat_n2id = {c.name: c.id for c in self.categories.values()}
        final = [cat_n2id.get(cid, cid) for cid in cat_list]
        return final

    def _subtotal_headings(self, operation, name, categories, anchor, negative=None):
        # Check if already exists any insertions
        view = self.resource.body["view"]
        if "transform" in view:
            insertions = view["transform"]["insertions"][:]
        else:
            insertions = []

        if categories:
            # allow categories to be a int or a category name
            if isinstance(categories, int) or isinstance(categories, str):
                categories = [categories]

            # Convert category names to id's if no id's where passed
            categories = self.cat_names_to_ids(categories)

            insertion = {
                "anchor": anchor,
                "name": name,
                "args": categories,
                "function": operation,
            }
            if negative:
                negative = self.cat_names_to_ids(negative)
                insertion["kwargs"] = {"negative": negative}
            insertions.append(insertion)

        payload = {
            'view': {
                'transform': {
                    'insertions': insertions
                }
            }
        }
        self.resource.patch(payload)
        self.dataset._reload_variables()
        return self.dataset[self.alias]

    def add_subtotal(self, name, categories=None, anchor=None):
        """
        :param: name: Name for the displayed subtotal
        :param: categories: a list of categories ID's or category Names to group in a heading.
            * Categories, when only one value, can be passed as single arguments as:
                categories=1 or categories='var_age'
            * Passing categories=None will remove all subtotals from the variable.
        :param: anchor: anchor can be any of, ['top', 'bottom', <category_id>].
            if the anchor isn't any of the above, it will default to be shown
            at the bottom of the last category ID specified in categories.

        Note: to concatenate subtotals the procedure requires to reassign the variable:
        var = var.add_subtotal('This is subtotal', [1, 2], 'top')
        var = var.add_subtotal('At the bottom', [3], 'bottom')
        var = ...
        """
        return self._subtotal_headings('subtotal', name, categories, anchor)

    def add_subtotal_difference(self, name, add, subtract, anchor):
        """
        :param: name: Name for the displayed subtotal
        :param: add: a list of categories ID's or category Names to group in a heading.
        :param: subtract: List of category IDs or Names to be subtracted from the added terms
        :param: anchor: anchor can be any of, ['top', 'bottom', <category_id>].
            if the anchor isn't any of the above, it will default to be shown
            at the bottom of the last category ID specified in categories.

        Note: to concatenate subtotals the procedure requires to reassign the variable:
        var.add_subtotal_difference("F - M", add=["Female"], subtract=["Male"], anchor="bottom")
        """
        return self._subtotal_headings('subtotal', name, add, anchor, subtract)

    def add_heading(self, name, categories=None, anchor=None):
        """
        :param: name: Name for the displayed subtotal
        :param: categories: a list of categories ID's or category Names to group in a heading.
            * Categories, when only one value, can be passed as single arguments as:
                categories=1 or categories='var_age'
            * Passing categories=None will remove all subtotals from the variable.
        :param: anchor: anchor can be any of, ['top', 'bottom', <category_id>].
            if the anchor isn't any of the above, it will default to be shown
            at the bottom of the last category ID specified in categories.

        Note: to concatenate headings the procedure requires to reassign the variable:
        var = var.add_heading('This is subtotal', [1, 2], 'top')
        var = var.add_heading('At the bottom', [3], 'bottom')
        var = ...
        """
        return self._subtotal_headings('heading', name, categories, anchor)

    def transformations(self):
        if 'transform' in self.view:
            return self.view.transform.insertions
        return None

    def edit_resolution(self, resolution):
        """
        PATCHes the rollup_resolution attribute of a datetime variable. This is the
        equivalent to the UI's change resolution action.

        Be sure to grab the editor's lock of the dataset.

        :usage: edited_var = var.edit_resolution('M')
        assert editar_var.rollup_resolution == 'M'
        """
        assert self.type == 'datetime', 'Method only allowed for datetime variables'
        self.dataset._validate_vartypes(self.type, resolution=resolution)
        view = self.view
        view['rollup_resolution'] = resolution
        self.resource.edit(view=view)
        return self


class BackfillFromCSV:
    """
    Performs backfilling of cells in a dataset given a CSV with the new values.

    Works by uploading the CSV to a temporary dataset and joining it by PK
    to the dataset with gaps.

    Will perform a `fill` operation for each of the aliases in the CSV file
    from the joined columns to the target ones.

    After all variables have been backfilled, the joined variables and tmp
    dataset are deleted.

    """
    TIMEOUT = 60 * 10  # 10 minutes

    def __init__(self, dataset, pk_alias, aliases, rows_expr, timeout=None):
        self.root = _default_connection(None)
        self.dataset = dataset
        self.aliases = set(aliases)
        self.pk_alias = pk_alias
        self.rows_expr = rows_expr
        self.alias_to_url = self.load_vars_by_alias()
        self.tmp_aliases = {
            a: "{}-{}".format(dataset.id, a) for a in aliases
        }
        self.progress_tracker = DefaultProgressTracking(timeout or self.TIMEOUT)
        self.timestamp = datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S")

    def load_vars_by_alias(self):
        """
        Returns a dict mapping each variable to its url and also
        fills it up for subvariables
        """
        ds = self.dataset.resource
        alias_to_url = {}
        for alias, vdef in ds.variables.by("alias").items():
            alias_to_url[alias] = vdef.entity_url
            if "subvariables" in vdef:
                for sv_alias, svdef in vdef.entity.subvariables.by("alias").items():
                    alias_to_url[sv_alias] = svdef.entity_url

        return alias_to_url

    def obtain_schema(self):
        md = self.dataset.resource.schema.metadata
        schema = {self.pk_alias: md[self.pk_alias]}
        schema.update({a: md[a] for a in self.aliases if a in md})
        subvar_aliases = self.aliases.difference(schema)
        for array_alias, vdef in md.items():
            if "subvariables" not in vdef:
                continue
            array_subvar_aliases = {v["alias"] for v in vdef["subvariables"]}
            for a in array_subvar_aliases.intersection(subvar_aliases):
                svdef = {
                    "alias": a,
                    "name": a,
                    "type": "categorical",
                    "categories": vdef["categories"],
                }
                schema[a] = svdef
        return schema

    def create_tmp_ds(self, csv_file):
        """
        Creates a new pycrunch dataset:

         * Creates using the schema for the corresponding variables to backfill
         * Uploads the CSV file
         * Renames the variables to disambiguate on the join
        """
        tmp_name = "Scrunch-backfill-{}-{}-{}".format(
            self.dataset.name, self.dataset.id, self.timestamp)

        # Create the new tmp dataset with the schema for the variables
        # from the target dataset. To ensure they are all the same type
        metadata = self.obtain_schema()
        tmp_ds = self.root.datasets.create(shoji_entity_wrapper({
            "name": tmp_name,
            "table": {
                "element": "crunch:table",
                "metadata": metadata
            }
        })).refresh()
        try:
            importing.importer.append_csv_string(tmp_ds, csv_file)
        except TaskError as err:
            raise ValueError(err.args[0])
        except pycrunch.ClientError as exc:
            # Error importing CSV file
            tmp_ds.delete()
            if exc.status_code == 400:
                # This is a validation error from the server
                raise ValueError("Error importing CSV file - Columns should match specified types")
            elif exc.status_code == 413:
                raise ValueError("Upload failed because the CSV file is too large. Limit is 150MB")
            else:
                # Other kind of error. Probably 413, or other kind. Don'w
                # swallow it. Expose it.
                raise

        # Rename the aliases in the tmp dataset to disambiguate on the join
        tmp_aliases = tmp_ds.variables.by("alias")
        tmp_ds.variables.patch(shoji_catalog_wrapper({
            tmp_aliases[a].entity_url: {"alias": self.tmp_aliases[a]} for a in self.aliases
        }))
        return tmp_ds

    def join_tmp_ds(self, tmp_ds):
        """
        We will perform the join with the presumption that both datasets
        have the same PK alias.
        """
        pk_url = self.alias_to_url[self.pk_alias]
        tmp_pk_url = tmp_ds.variables.by("alias")[self.pk_alias].entity_url
        join_payload = shoji_entity_wrapper(
            {
                "function": "adapt",
                "args": [
                    {"dataset": tmp_ds.self},
                    {"variable": tmp_pk_url},
                    {"variable": pk_url},
                ],
            }
        )
        resp = self.dataset.resource.variables.post(join_payload)
        pycrunch.shoji.wait_progress(resp, self.dataset.resource.session,
                                     progress_tracker=self.progress_tracker)

    def backfill(self):
        variables_expr = {}

        # We need to fetch the variables dictionary again since it's going
        # to be re-read after the join to include the new variables.
        joined_vars_by_alias = self.dataset.resource.variables.by("alias")

        # Replace with the straight column. This is a safe operation because
        # we know by definition the types of the incoming columns have the exact
        # same type as the targets
        for alias in self.aliases:
            var_w_gaps = self.alias_to_url[alias]
            var_w_values = joined_vars_by_alias[self.tmp_aliases[alias]].entity_url
            variables_expr[var_w_gaps] = {"variable": var_w_values}

        # We can perform an update command here because we're guaranteed
        # that the types for each of the variables matches the column we
        # want to backfill.
        update_expr = {
            "command": "update",
            "variables": variables_expr,
        }
        if self.rows_expr:
            update_expr["filter"] = self.rows_expr
        with NoExclusion(self.dataset) as ds:
            resp = ds.resource.table.post(update_expr)

        # Continue handling this outside of the `with` block, so the exclusion
        # filter gets re-applied while we wait.
        if resp.status_code == 202:
            # If the response was async. Wait for it finishing
            pycrunch.shoji.wait_progress(resp, self.dataset.resource.session,
                                         progress_tracker=self.progress_tracker)

    def execute(self, csv_file):
        # Create a new dataset with the CSV file, We want this TMP dataset
        # to have the same types as the variables we want to replace.
        tmp_ds = self.create_tmp_ds(csv_file)
        sp_msg = "Savepoint before backfill: {}".format(self.timestamp)
        with SavepointRestore(self.dataset, sp_msg):
            try:
                self.join_tmp_ds(tmp_ds)
                self.backfill()
            finally:
                # Delete the joined variables
                folder_name = tmp_ds.body["name"]
                folders_by_name = self.dataset.resource.folders.public.by("name")
                if folder_name in folders_by_name:
                    folders_by_name[folder_name].entity.delete()
                # Always delete the tmp dataset no matter what
                tmp_ds.delete()


