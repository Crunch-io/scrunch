import json
import requests
import re
import six

if six.PY2:  # pragma: no cover
    from urlparse import urlsplit
else:
    from urllib.parse import urlsplit

import pandas as pd

import pycrunch
from pycrunch.importing import Importer
from pycrunch.shoji import wait_progress
from pycrunch.exporting import export_dataset

from scrunch.expressions import parse_expr, process_expr
from scrunch.variables import validate_variable_url


SKELETON = {
    'element': 'shoji:entity',
    'body': {
        'name': 'name',
        'description': 'description',
        'alias': 'alias',
        'expr': {
            'function': 'function',
            'args': []
        }
    }
}

REQUIRED_VALUES = {'name', 'id', 'missing', 'combined_ids'}
REQUIRES_RESPONSES = {'combined_ids', 'name'}
SUBVAR_ALIAS = re.compile(r'.+_(\d+)$')


def subvar_alias(parent_alias, response_id):
    return '%s_%d' % (parent_alias, response_id)


def responses_from_map(variable, response_map, alias, parent_alias):
    subvars = variable.resource.subvariables.by('alias')
    responses = [{
                     'name': combined['name'],
                     'alias': subvar_alias(alias, combined['id']),
                     'combined_ids': [subvars[subvar_alias(parent_alias,
                         sv_alias)].entity_url for sv_alias in
                                      combined['combined_ids']]
                 } for combined in response_map]
    return responses


def combinations_from_map(category_map):
    combinations = [{
                        'name': combined['name'],
                        'missing': combined.get('missing', False),
                        'combined_ds': combined['combined_ids']
                    } for combined in category_map]
    return combinations


def combine_responses_expr(variable, responses):
    return {
        'function': 'combine_responses',
        'args': [{
            'variable': variable.resource.self
        }, {
            'value': responses
        }]
    }


def combine_categories_expr(variable, combinations):
    return {
        'function': 'combine_categories',
        'args': [{
            'variable': variable.resource.self
        }, {
            'value': combinations
        }]
    }


def get_dataset(dataset, site=None):
    """
    Retrieve a reference to a given dataset (either by name, or ID) if it exists.
    This method uses the library singleton session if the optional "site"
    parameter is not provided.

    Returns a Dataset Entity record if the dataset exists.
    Raises a KeyError if no such dataset exists.
    """
    if site is None:
        if pycrunch.session is None:
            raise AttributeError("Authenticate first with scrunch.connect()")
        site = pycrunch.session
    try:
        shoji_ds = site.datasets.by('name')[dataset].entity
    except KeyError:
        shoji_ds = site.datasets.by('id')[dataset].entity
    return Dataset(shoji_ds)


def change_project(project, site=None):
    """
    :param project: name or ID of the project
    :param site: scrunch session, defaults to global session
    :return: the project session
    """
    if site is None:
        if pycrunch.session is None:
            raise AttributeError("Authenticate first with scrunch.connect()")
        site = pycrunch.session
    try:
        ret = site.projects.by('name')[project].entity
    except KeyError:
        ret = site.projects.by('id')[project].entity
    pycrunch.session = ret


def create_dataset(name, variables, site=None):
    if site is None:
        if pycrunch.session is None:
            raise AttributeError("Authenticate first with scrunch.connect()")
        site = pycrunch.session

    shoji_ds = site.datasets.create({
        'element': 'shoji:entity',
        'body': {
            'name': name,
            'table': {
                'element': 'crunch:table',
                'metadata': variables
            }
        }
    }).refresh()
    return Dataset(shoji_ds)


def var_name_to_url(ds, alias):
    """
    :param ds: The dataset we are gonna inspect
    :param alias: the alias of the variable name we want to check
    :return: the id of the given varname or None
    """
    try:
        return ds.variables.by('alias')[alias].entity.self
    except KeyError:
        raise KeyError(
            'Variable %s does not exist in Dataset %s' % (alias,
                                                          ds['body']['name']))


def var_id_to_url(ds, id):
    """
    :param ds: The dataset to look for the id of variable
    :param id: The id string of a variable
    :return: the url of the given variable as crunch url
    """
    try:
        return ds.variables.by('id')[id].entity.self
    except KeyError:
        raise KeyError(
            'Variable %s does not exist in Dataset %s' % (id,
                                                          ds['body']['name']))


def variable_to_url(ds, variable):
    """Receive a valid variable reference and return the variable url.

    :param ds: The crunch dataset
    :param variable: A valid variable reference in the form of a shoji Entity
                     of the variable or a string containing the variable url
                     or alias.
    :return: The variable url
    """
    assert isinstance(variable, (six.string_types, Variable))

    if isinstance(variable, Variable):
        return variable.resource.self

    elif validate_variable_url(variable):
        return variable
    else:
        try:
            return var_name_to_url(ds, variable)
        except KeyError:
            return var_id_to_url(ds, variable)


def aliases_to_urls(ds, variable_url, response_map):
    """
    Maps subvariable aliases to urls
    :param ds: a dataset object
    :param variable_url: url of the variable we want to inspect
    :param response_map: mapping of new subvariables
    :return:
    """
    suvars = ds.variables.index(variable_url).entity.subvariables.by('alias')
    mapped_urls = {}
    for key, values in response_map.items():
        try:
            mapped_urls[key] = [suvars[x].entity_url for x in values]
        except KeyError:
            raise KeyError(
                'Unexistent variables %s in Dataset %s' % (
                    values, ds['body']['alias']))
    return mapped_urls


def validate_category_rules(categories, rules):
    if not ((len(categories) - 1) <= len(rules) <= len(categories)):
        raise ValueError(
            'Amount of rules should match categories (or categories -1)'
        )


def validate_category_map(map):
    """
    :param map: categories keyed by new category id mapped to existing ones
    :return: a list of dictionary objects that the Crunch API expects
    """
    for value in map.values():
        keys = set(list(value.keys()))
        assert keys & REQUIRED_VALUES, (
            'category_map has one or more missing keys of ' % REQUIRED_VALUES)
    rebuilt = list()
    for key, value in map.items():
        category = dict()
        category.update(value)
        # unfold expressions like range(1,5) to a list of ids
        category['combined_ids'] = list(category['combined_ids'])
        category['id'] = key
        rebuilt.append(category)
    return rebuilt


def validate_response_map(map):
    """
    :param map: responses keyed by new alias mapped to existing aliases
    :return: a list of dictionaries describing the new responses to create for
             the variable
    """
    rebuilt = list()
    for key, value in map.items():
        response = dict()
        response['name'] = key
        response['combined_ids'] = value
        rebuilt.append(response)
    return rebuilt


def download_file(url, filename):
    if url.startswith('file://'):
        # Result is in local filesystem (for local development mostly)
        import shutil
        shutil.copyfile(url.split('file://', 1)[1], filename)
    else:
        r = requests.get(url, stream=True)
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:   # filter out keep-alive new chunks
                    f.write(chunk)
    return filename


def case_expr(rules, name, alias):
    """
    Given a set of rules, return a `case` function expression to create a
     variable.
    """
    expression = {
        'references': {
            'name': name,
            'alias': alias,
        },
        'function': 'case',
        'args': [{
            'column': [1, 2],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': [
                        {'id': 1, 'name': 'Selected', 'missing': False, 'numeric_value': None, 'selected': True},
                        {'id': 2, 'name': 'Not selected', 'missing': False, 'numeric_value': None, 'selected': False},
                    ]
                }
            }
        }]
    }
    expression['args'].append(rules)
    return expression


class Dataset(object):
    """
    A pycrunch.shoji.Entity wrapper that provides dataset-specific methods.
    """

    ENTITY_ATTRIBUTES = {'id', 'name', 'notes', 'descrpition', 'is_published',
                         'archived', 'end_date', 'start_date', 'creation_time',
                         'modification_time'}

    def __init__(self, resource):
        """
        :param resource: Points to a pycrunch Shoji Entity for a dataset.
        """
        self.resource = resource
        self.session = self.resource.session

    def __getattr__(self, item):
        if item in self.ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

        # Check if the attribute corresponds to a variable alias
        variable = self.resource.variables.by('alias').get(item)

        if variable is None:
            # Variable doesn't exists, must raise an AttributeError
            raise AttributeError('Dataset has no attribute %s' % item)

        # Variable exists!, return the variable entity
        return variable.entity

    def __getitem__(self, item):
        # Check if the attribute corresponds to a variable alias
        variable = self.resource.variables.by('alias').get(item)
        if variable is None:
            # Variable doesn't exists, must raise an ValueError
            raise ValueError('Dataset has no variable %s' % item)

        # Variable exists!, return the variable entity
        return Variable(variable.entity)

    def rename(self, new_name):
        self.resource.edit(name=new_name)

    def stream_rows(self, columns):
        """
        Receives a dict with columns of values to add and streams them
         into the dataset. Client must call .push_rows(n) later.

        Returns the total of rows streamed
        """
        importer = Importer()
        count = len(columns.values()[0])
        for x in range(count):
            importer.stream_rows(self.resource, {a: columns[a][x] for a in columns})
        return count

    def push_rows(self, count):
        """
        Batches in the rows that have been currently streamed.
        """
        self.resource.batches.create({
            'element': 'shoji:entity',
            'body': {
                'stream': count,
                'type': 'ldjson'
            }
        })

    def exclude(self, expr=None):
        """
        Given a dataset object, apply an exclusion filter to it (defined as an
        expression string).

        If the `expr` parameter is None, an empty expression object is sent
        as part of the PATCH request, which effectively removes the exclusion
        filter (if any).
        """
        if isinstance(expr, six.string_types):
            expr_obj = parse_expr(expr)
            expr_obj = process_expr(expr_obj, self.resource)  # cause we need URLs
        elif expr is None:
            expr_obj = {}
        else:
            expr_obj = expr
        return self.resource.session.patch(
            self.resource.fragments.exclusion,
            data=json.dumps(dict(expression=expr_obj))
        )

    def create_categorical(self, categories,
                           name, alias, description='', missing=True):
        """
        Creates a categorical variable deriving from other variables.
        Uses Crunch's `case` function.
        """
        rules = []
        for cat in categories:
            rules.append(cat.pop('rules'))

        if not hasattr(self.resource, 'variables'):
            self.resource.refresh()

        args = [{
            'column': [c['id'] for c in categories],
            'type': {
                'value': {
                    'class': 'categorical',
                    'categories': categories}}}]

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
        for rule in rules:
            more_args.append(parse_expr(rule))

        more_args = process_expr(more_args, self.resource)

        expr = dict(function='case', args=args + more_args)

        payload = dict(element='shoji:entity',
                       body=dict(alias=alias,
                                 name=name,
                                 expr=expr,
                                 description=description))

        return Variable(self.resource.variables.create(payload))

    def create_multiple_response(self, responses, name, alias, description=''):
        """
        Creates a Multiple response (array) using a set of rules for each
         of the responses(subvariables).
        """
        responses_map = {}
        for resp in responses:
            rules = resp['rules']
            if isinstance(rules, basestring):
                rules = process_expr(parse_expr(rules), self.resource)
            responses_map[str(resp['id'])] = case_expr(rules, name=resp['name'],
                alias='%s_%d' % (alias, resp['id']))

        payload = {
            'element': 'shoji:entity',
            'body': {
                'name': name,
                'alias': alias,
                'description': description,
                'derivation': {
                    'function': 'array',
                    'args': [{
                        'function': 'select',
                        'args': [{
                            'map': responses_map
                        }]
                    }]
                }
            }
        }
        return Variable(self.resource.variables.create(payload).refresh())

    def copy_variable(self, variable, name, alias):
        payload = {
            'element': 'shoji:entity',
            'body': {
                'name': name,
                'alias': alias,
                'derivation': {
                    'function': 'copy_variable',
                    'args': [{
                        'variable': variable.resource.self
                    }]
                }
            }
        }
        if variable.type == 'multiple_response':
            # In the case of MR variables, we want the copies' subvariables to
            # have their aliases in the same pattern and order that the parent's
            # are, that is `parent_alias_#`.
            subreferences = []
            for subvar in variable.resource.subvariables.index.values():
                alias = subvar['alias']
                match = SUBVAR_ALIAS.match(alias)
                if match:  # Does this var have the subvar pattern?
                    suffix = match.groups()[0]  # Keep the position
                    alias = subvar_alias(alias, suffix)

                subreferences.append({
                    'name': subvar['name'],
                    'alias': alias
                })
            payload['body']['derivation']['references'] = {
                'subreferences': subreferences
            }
        shoji_var = self.resource.variables.create(payload).refresh()
        return Variable(shoji_var)

    def combine(self, variable, category_map, name, alias, description=''):
        if variable.type in 'multiple_response':
            return self.combine_responses(variable, category_map, name=name,
                alias=alias, description=description)
        else:
            return self.combine_categories(variable, category_map, name=name,
                alias=alias, description=description)

    def combine_categories(self, variable, category_map,
                           name, alias, description=''):
        """
        Create a new variable in the given dataset that is a recode
        of an existing variable
        category_map = {
            1: {
                "name": "Favorable",
                "missing": True,
                "combined_ids": [1,2]
            },
        }
        :param variable: alias of the variable to recode
        :param name: name for the new variable
        :param alias: alias for the new variable
        :param description: description for the new variable
        :param category_map: map to combine categories
        :return: the new created variable
        """
        if isinstance(variable, basestring):
            variable = self[variable]

        combinations = combinations_from_map(category_map)
        payload = SKELETON.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['derivation'] = combine_categories_expr(variable,
            combinations)
        return Variable(self.resource.variables.create(payload).refresh())

    def combine_responses(self, variable, response_map,
                          name, alias, description=''):
        """
        Creates a new variable in the given dataset that combines existing
        responses into new categorized ones

        response_map = [
            {"id": 1, "name": 'online', 'combined_ids': [1]},
            {"id": 2, "name": 'notonline', 'combined_ids': [2, 3, 4]}
        ]
        :return: newly created variable
        """
        if isinstance(variable, basestring):
            parent_alias = variable
            variable = self[variable]
        else:
            parent_alias = variable.alias

        responses = responses_from_map(variable, response_map, alias,
            parent_alias)
        payload = SKELETON.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['derivation'] = combine_responses_expr(variable,
            responses)
        return Variable(self.resource.variables.create(payload))

    def change_editor(self, user):
        """
        Change the current editor of the Crunch dataset.

        Parameters
        ----------
        user : str
            The email address or the crunch url of the user who should be set
            as the new current editor of the given dataset.

        Returns
        -------
        None
        """

        def _host_from_url(url):
            resolved = urlsplit(url)
            return resolved.hostname

        def _to_url(email):
            api_users = 'https://{}/api/users/'.format(
                _host_from_url(self.resource.self)
            )
            user_url = None

            users = self.session.get(api_users).payload['index']

            for url, user in six.iteritems(users):
                if user['email'] == email:
                    user_url = url
                    self.patch({'current_editor': url})
                    break
            assert user_url is not None, 'Unable to resolve user url'

            return user_url

        def _is_url(u):
            return u.startswith('https://') or u.startswith('http://')

        user_url = user if _is_url(user) else _to_url(user)

        self.resource.patch({'current_editor': user_url})

    def create_savepoint(self, description):
        """
        Creates a savepoint on the dataset.

        Parameters
        ----------
        description : str
            The description that should be given to the new savepoint. This
            function will not let you create a new savepoint with the same
            description as any other savepoint.

        Returns
        -------
        None
        """
        if len(self.resource.savepoints.index) > 0:
            if description in self.savepoint_attributes('description'):
                raise KeyError(
                    "A checkpoint with the description '{}' already"
                    " exists.".format(description)
                )

        self.resource.savepoints.create({
            'element': 'shoji:entity',
            'body': {'description': description}
        })

    def load_savepoint(self, description=None):
        """
        Load a savepoint on the dataset.

        Parameters
        ----------
        description : str, default=None
            The description that identifies which savepoint to be loaded.
            When loading a savepoint, all savepoints that were saved after
            the loaded savepoint will be destroyed permanently.

        Returns
        -------
        None
        """

        if description is None:
            description = 'initial import'
        elif description not in self.savepoint_attributes('description'):
            raise KeyError(
                "No checkpoint with the description '{}'"
                " exists.".format(description)
            )

        revert = self.resource.savepoints.by('description').get(description).revert
        self.resource.session.post(revert)

    def savepoint_attributes(self, attrib):
        """
        Return list of attributes from the given dataset's savepoints.

        Parameters
        ----------
        attrib : str
            The attribute to be returned for each savepoint in the given
            dataset. Available attributes are:
                'creation_time'
                'description'
                'last_update'
                'revert'
                'user_name'
                'version'
        """

        if len(self.resource.savepoints.index) == 0:
            return []
        else:
            attribs = [
                cp[attrib]
                for url, cp in six.iteritems(self.resource.savepoints.index)
            ]

            return attribs

    def fork(self, description=None, name=None, is_published=False,
             preserve_owner=False, **kwargs):
        """
        Create a fork of ds and add virgin savepoint.

        Parameters
        ----------
        description : str, default=None
            If given, the description to be applied to the fork. If not
            given the description will be copied from ds.
        name : str, default=None
            If given, the name to be applied to the fork. If not given a
            default name will be created which numbers the fork based on
            how many other forks there are on ds.
        is_published : bool, default=False
            If True, the fork will be visible to viewers of ds. If False it
            will only be viewable to editors of ds.
        preserve_owner : bool, default=False
            If True, the owner of the fork will be the same as the parent
            dataset. If the owner of the parent dataset is a Crunch project,
            then it will be preserved regardless of this parameter.

        Returns
        -------
        _fork : scrunch.datasets.Dataset
            The forked dataset.
        """

        nforks = len(self.resource.forks.index)
        if name is None:
            name = "FORK #{} of {}".format(nforks + 1, self.resource.body.name)
        if description is None:
            description = self.resource.body.description

        body = dict(
            name=name,
            description=description,
            is_published=is_published,
            **kwargs
        )
        # not returning a dataset
        _fork = self.resource.forks.create({"body": body}).refresh()
        fork = Dataset(_fork)
        fork.create_savepoint("initial fork")

        if preserve_owner or '/api/projects/' in self.resource.body.owner:
            try:
                _fork.patch({'owner': self.resource.body.owner})
                _fork.refresh()
            except (pycrunch.ClientError, pycrunch.ServerError):
                # Don't fail if the fork's owner cannot be set to be the same
                # as the parent dataset.
                pass

        return fork

    def forks_dataframe(self):
        """
        Return a dataframe summarizing the forks on the dataset.

        Returns
        -------
        _forks : pandas.DataFrame
            A DataFrame representation of all attributes from all forks
            on the given dataset.
        """

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
            _forks['modification_time'] = pd.to_datetime(_forks['modification_time'])
            _forks.sort(columns='creation_time', inplace=True)

            return _forks

    def delete_forks(self):
        """
        Deletes all the forks on the dataset. CANNOT BE UNDONE!
        """

        for fork in six.itervalues(self.resource.forks.index):
            fork.entity.delete()

    def download(self, path, filter=None, variables=None, include_hidden=True):
        """
        Downloads a dataset as CSV to the given path.
        this includes hidden variables and categories
        as id's.
        """
        # the payload should include all hidden variables by default
        payload = {
            "element": "shoji:entity",
            "body": {
                "options": {"use_category_ids": True}
            }
        }
        # add filter to rows if passed
        if filter:
            payload['body']['filter'] = process_expr(
                parse_expr(filter), self.resource)
        # convert variable list to crunch identifiers
        if variables and isinstance(variables, list):
            id_vars = []
            for var in variables:
                id_vars.append(variable_to_url(self.resource, var))
            # Now build the payload with selected variables
            payload['body']['where'] = {
                    'function': 'select',
                    'args': [{
                        'map': {
                            x: {'variable': x} for x in id_vars
                        }
                    }]
                }
        # include_hidden is mutually exclusive with
        #  variables to include in the download
        if include_hidden and not variables:
            payload['body']['where'] = {
                    'function': 'select',
                    'args': [{
                        'map': {
                            x: {'variable': x} for x in self.resource.variables.index.keys()
                        }
                    }]
                }
        url = export_dataset(self.resource, payload, format='csv')
        download_file(url, path)

    def join(self, left_var, right_ds, right_var, columns=None,
             filter=None, wait=True):
        """
        Joins a given variable. In crunch joins are left joins, where
        left is the dataset variable and right is other dataset variable.
        For more information see:
        http://docs.crunch.io/?http#merging-and-joining-datasets

        :param: columns: Specify a list of variables from right dataset
        to bring in the merge:
        http://docs.crunch.io/?http#joining-a-subset-of-variables

        :param: wait: Wait for the join progress to finish by polling
        or simply return a url to the progress resource

        :param: filter: Filters out rows based on the given expression,
        or on a given url for an existing filter. TODO: for the moment
        we only allow expressions
        """
        right_var_url = var_name_to_url(right_ds, right_var)
        left_var_url = var_name_to_url(self, left_var)
        # this dictionary sets the main part of the join
        adapter = {
            'function': 'adapt',
            'args': [
                {'dataset': right_ds.self},
                {'variable': right_var_url},
                {'variable': left_var_url}
            ]
        }
        # wrap the adapter method on a shoji and body entity
        payload = {
            'element': 'shoji:entity',
            'body': adapter
        }

        if columns and isinstance(columns, list):
            # overwrite body to new format
            payload['body'] = {
                'frame': adapter,
                'function': 'select',
                'args': [
                    {'map': {}}
                ]
            }
            # add the individual variable columns to the payload
            for var in columns:
                var_url = var_name_to_url(right_ds, var)
                payload['body']['args'][0]['map'][var_url] = {'variable': var_url}

        if filter:
            # in the case of a filter, convert it to crunch
            # and attach the filter to the payload
            expr = process_expr(parse_expr(filter), right_ds)
            payload['body']['filter'] = {'expression': expr}

        progress = self.variables.post(payload)
        # poll for progress to finish or return the url to progress
        if wait:
            return wait_progress(r=progress, session=self.session, entity=self)
        return progress.json()['value']

    def recode(self, categories, alias, name, multiple, description=''):
        """
        Used to create new categorical variables using Crunchs's `case` function.

         Will create either categorical variables or multiple response depending
         on the `multiple` parameter.
        """
        if multiple:
            return self.create_multiple_response(categories, alias=alias,
                name=name, description=description)
        else:
            return self.create_categorical(categories, alias=alias, name=name,
                description=description)


class Variable(object):
    """
    A pycrunch.shoji.Entity wrapper that provides variable-specific methods.
    """

    ENTITY_ATTRIBUTES = {'name', 'alias', 'description', 'discarded', 'format',
                         'type', 'id', 'view', 'notes'}

    def __init__(self, resource):
        self.resource = resource

    def __getattr__(self, item):
        if item in self.ENTITY_ATTRIBUTES:
            return self.resource.body[item]  # Has to exist

    def combine(self, alias=None, map=None, names=None, default='missing',
               name=None, description=None):
        # DEPRECATED - USE Dataset.combine*
        """
        Implements SPSS-like recode functionality for Crunch variables.

        This method combines Crunch's `combine_categories` and
        `combine_responses` in a single method when applied to a variable
        that is deemed as ~categorical~ by the user.
        """
        if alias is None:
            raise TypeError('Missing alias for the recoded variable')
        if map is None:
            raise TypeError('Missing recode map')
        if not isinstance(map, dict) or not map:
            raise ValueError('Invalid recode map')
        if default not in ('missing', 'copy'):
            raise ValueError(
                'The "default" argument must be either "missing" or "copy"'
            )

        if 'body' not in self.resource:
            self.resource.refresh()

        if self.resource.body.type not in ('categorical', 'categorical_array',
                                           'multiple_response'):
            raise TypeError(
                'Only categorical, categorical_array and multiple_response '
                'variables are supported'
            )

        if name is None:
            name = self.resource.body.name + ' (recoded)'

        if description is None:
            description = self.resource.body.description

        if self.resource.body.type in ('categorical', 'categorical_array'):
            # On this case perform a `combine_categories` operation

            if names is None:
                raise TypeError('Missing category names')

            processed_categories = list()
            category_defs = list()
            category_defs_by_id = dict()
            default_name = 'Missing'
            existing_categories_by_id = dict()
            existing_categories_by_name = dict()
            for existing_category in self.resource.body.get('categories', []):
                _id = existing_category['id']
                _name = existing_category['name']
                existing_categories_by_id[_id] = existing_category
                existing_categories_by_id[_name] = existing_category

            # 1. Basic category definitions.
            for _id, value in map.items():
                if isinstance(_id, int):
                    if not isinstance(value, (list, tuple, int,
                                              six.string_types, range)):
                        raise ValueError('Invalid mapped value')
                    if isinstance(value, (int, six.string_types)):
                        value = [value]

                    processed_value = []
                    for element in value:
                        if isinstance(element, six.string_types):
                            try:
                                element = existing_categories_by_name[element]
                            except KeyError:
                                raise ValueError(
                                    'Invalid category name %s' % element
                                )
                            assert isinstance(element, int)
                        if isinstance(element, int):
                            if element not in existing_categories_by_id:
                                raise ValueError(
                                    'Invalid numeric code %s' % element
                                )
                        else:
                            raise ValueError(
                                'Invalid mapped value %s' % element
                            )
                        processed_value.append(element)

                    category = {
                        'id': _id,
                        'name': str(_id),
                        'missing': False,
                        'combined_ids': processed_value
                    }
                    category_defs.append(category)
                    category_defs_by_id[_id] = category
                    processed_categories.extend(list(value))
            category_defs = sorted(category_defs, key=lambda c: c['id'])

            # 2. Add category names (if defined).
            if type(names) in (list, tuple):
                for i, category_name in enumerate(names):
                    if i < len(category_defs):
                        category_defs[i]['name'] = category_name
                    elif i == len(category_defs):
                        default_name = category_name
            elif isinstance(names, dict):
                for _id, category_name in names.items():
                    if _id in category_defs_by_id:
                        category_defs_by_id[_id]['name'] = category_name

            # 3. Add the "missing" stuff.
            missing_category = {
                'id': max(category_defs_by_id.keys()) + 1,
                'name': default_name,
                'missing': True,
                'combined_ids': []
            }
            for existing_category in self.resource.body.get('categories', []):
                _id = existing_category['id']
                if _id not in processed_categories:
                    if default == 'missing':
                        missing_category['combined_ids'].append(_id)
                    elif default == 'copy':
                        category = {
                            'id': _id,
                            'name': existing_category['name'],
                            'missing': existing_category['missing'],
                            'combined_ids': [_id]
                        }
                        category_defs.append(category)
                        category_defs_by_id[_id] = category
                    processed_categories.append(_id)

            if default == 'missing':
                category_defs.append(missing_category)

            # 4. Create the recoded variable.
            payload = SKELETON.copy()
            payload['body']['name'] = name
            payload['body']['alias'] = alias
            payload['body']['description'] = description
            payload['body']['expr']['function'] = 'combine_categories'
            payload['body']['expr']['args'] = [
                {
                    'variable': self.resource['self']
                },
                {
                    'value': category_defs
                }
            ]
        else:  # multiple_response
            # Perform a `combine_responses` derivation
            subreferences = self.resource.body.get('subreferences', [])
            subvariables = self.resource.body.get('subvariables', [])
            assert len(subreferences) == len(subvariables)

            # 1. Gather the URLs of the subvariables.
            subvar_urls = {
                subvar['alias']: subvariables[i]
                for i, subvar in enumerate(subreferences)
            }

            # 2. Generate the list of response definitions for the recoded
            #    variable.
            response_defs = list()
            for new_subvar_name in sorted(map):
                value = map[new_subvar_name]
                if not isinstance(value, (list, tuple, six.string_types)):
                    raise ValueError
                if isinstance(value, six.string_types):
                    value = [value]
                try:
                    response = {
                        'name': new_subvar_name,
                        'combined_ids': [
                            subvar_urls[_alias] for _alias in value
                        ]
                    }
                    response_defs.append(response)
                except KeyError:
                    raise ValueError(
                        'Invalid subvariable alias(es) in %s' % value
                    )

            # 3. Create the recoded variable.
            payload = SKELETON.copy()
            payload['body']['name'] = name
            payload['body']['alias'] = alias
            payload['body']['description'] = description
            payload['body']['expr']['function'] = 'combine_responses'
            payload['body']['expr']['args'] = [
                {
                    'variable': self.resource['self']
                },
                {
                    'value': response_defs
                }
            ]

        ds = get_dataset(self.resource.body.dataset_id)
        return ds.variables.create(payload).refresh()

    def edit_combination(self, response_map):
        alias = self.alias
        if self.type == 'multiple_response':
            responses = responses_from_map(self, response_map, alias, alias)
            derivation = combine_responses_expr(self, responses)
        else:
            combinations = combinations_from_map(response_map)
            derivation = combine_categories_expr(self, combinations)
        self.resource.edit(derivation=derivation)

    def edit_categorical(self, categories, rules):
        # validate rules and categories are same size
        validate_category_rules(categories, rules)
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
        ds = get_dataset(self.resource.body.dataset_id)
        more_args = process_expr(more_args, ds)
        # epression value building
        expr = dict(function='case', args=args + more_args)
        payload = dict(
            element='shoji:entity',
            body=dict(expr=expr)
        )
        # patch the variable with the new payload
        return self.resource.patch(payload)

    def edit_derived(self, variable, mapper):
        # get some initial variables
        ds = get_dataset(self.resource.body.dataset_id)
        variable_url = variable_to_url(ds, variable)
        function = self.resource.body.derivation['function']

        # make the proper transformations based on the function
        # array is combine_responses
        if function == 'array':
            trans_responses = aliases_to_urls(ds, variable_url, mapper)
            values = validate_response_map(trans_responses)
            function = 'combine_responses'
        elif function == 'combine_categories':
            values = validate_category_map(mapper)
        else:
            raise AttributeError(
                'Function %s does not support edit' % function)

        # build the proper payload
        payload = {
            'element': 'shoji:entity',
            'body': {
                'expr': {
                    'function': function,
                    'args': [
                        {
                            'variable': variable_url
                        },
                        {
                            'value': values
                        }
                    ]
                }
            }
        }
        return self.resource.patch(payload)
