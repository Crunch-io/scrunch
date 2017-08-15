import collections
import datetime
import re
import six
import pycrunch
from pycrunch.importing import Importer
from pycrunch.shoji import wait_progress

from scrunch.expressions import parse_expr, process_expr
from scrunch.helpers import abs_url, case_expr, subvar_alias
from scrunch.exceptions import InvalidDatasetTypeError
from scrunch.datasets import (BaseDataset, _get_connection, _get_dataset,
                              Variable, CrunchBox)
from scrunch.subentity import Filter
from scrunch.variables import (combinations_from_map, combine_categories_expr,
                               combine_responses_expr, responses_from_map)

_VARIABLE_PAYLOAD_TMPL = {
    'element': 'shoji:entity',
    'body': {
        'name': 'name',
        'description': 'description',
        'alias': 'alias'
    }
}

_MR_TYPE = 'multiple_response'


def get_dataset(dataset, connection=None, editor=False):
    """
    A simple wrapper of _get_dataset with streaming=False
    """
    shoji_ds, root = _get_dataset(dataset, connection, editor, streaming=False)
    # make sure the Dataset is of type streaming != "streaming"
    if shoji_ds['body'].get('streaming') == 'streaming':
        raise InvalidDatasetTypeError("Dataset %s is of type 'streaming',\
            use get_streaming_dataset method instead" % dataset)
    ds = MutableDataset(shoji_ds)
    if editor is True:
        ds.change_editor(root.session.email)
    return ds


def create_dataset(name, variables, connection=None):
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")

    shoji_ds = connection.datasets.create({
        'element': 'shoji:entity',
        'body': {
            'name': name,
            'table': {
                'element': 'crunch:table',
                'metadata': variables
            }
        }
    }).refresh()
    return MutableDataset(shoji_ds)


class MutableDataset(BaseDataset):
    """
    Class that enclose mutable dataset methods or any
    method that varies the state of the dataset and/or it's data.
    """

    def edit(self, **kwargs):
        """
        Edit main Dataset attributes
        """
        for key in kwargs:
            if key not in self._MUTABLE_ATTRIBUTES:
                raise AttributeError("Can't edit attibute %s of variable %s" % (
                    key, self.name
                ))
            if key in ['start_date', 'end_date'] and \
                    (isinstance(kwargs[key], datetime.date) or
                     isinstance(kwargs[key], datetime.datetime)
                     ):
                kwargs[key] = kwargs[key].isoformat()

        return self.resource.edit(**kwargs)

    def delete(self):
        """
        Delete a dataset.
        """
        self.resource.delete()

    def stream_rows(self, columns):
        """
        Receives a dict with columns of values to add and streams them
        into the dataset. Client must call .push_rows(n) later or wait until
        Crunch automatically processes the batch.

        Returns the total of rows streamed
        """
        importer = Importer()
        count = len(list(columns.values())[0])
        for x in range(count):
            importer.stream_rows(self.resource,
                                 {a: columns[a][x] for a in columns})
        return count

    def push_rows(self, count=None):
        """
        Batches in the rows that have been recently streamed. This forces
        the rows to appear in the dataset instead of waiting for crunch
        automatic batcher process.
        """
        if bool(self.resource.stream.body.pending_messages):
            self.resource.batches.create({
                'element': 'shoji:entity',
                'body': {
                    'stream': count,
                    'type': 'ldjson'
                }
            })

    def fork(self, description=None, name=None, is_published=False,
             preserve_owner=False, **kwargs):
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
        :param preserve_owner: bool, default=False
            If True, the owner of the fork will be the same as the parent
            dataset. If the owner of the parent dataset is a Crunch project,
            then it will be preserved regardless of this parameter.

        :returns _fork: scrunch.datasets.Dataset
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
        fork = MutableDataset(_fork)

        if preserve_owner or '/api/projects/' in self.resource.body.owner:
            try:
                _fork.patch({'owner': self.resource.body.owner})
                _fork.refresh()
            except (pycrunch.ClientError, pycrunch.ServerError):
                # Don't fail if the fork's owner cannot be set to be the same
                # as the parent dataset.
                pass

        return fork

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
            # sometimes q is not a dict but simply a string, convert it to a dict
            if isinstance(q, str):
                q = {"query": q}
            as_json = {}
            parsed_q = process_expr(parse_expr(q['query']), self.resource)
            # wrap the query in a list of one dict element
            as_json['query'] = [parsed_q]
            if 'transform' in q.keys():
                as_json['transform'] = q['transform']
            parsed_template.append(as_json)
        payload = dict(element='shoji:entity',
                       body=dict(name=name,
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
        payload = dict(element='shoji:entity',
                       body=dict(name=name,
                                 multitable=multi.resource.self))
        self.resource.multitables.create(payload)
        return self.multitables[name]

    def create_single_response(self, categories, name, alias, description='',
                               missing=True, notes=''):
        """
        Creates a categorical variable deriving from other variables.
        Uses Crunch's `case` function.
        """
        cases = []
        for cat in categories:
            cases.append(cat.pop('case'))

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
        for case in cases:
            more_args.append(parse_expr(case))

        more_args = process_expr(more_args, self.resource)

        expr = dict(function='case', args=args + more_args)

        payload = dict(element='shoji:entity',
                       body=dict(alias=alias,
                                 name=name,
                                 expr=expr,
                                 description=description,
                                 notes=notes))

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return the variable instance
        return self[new_var['body']['alias']]

    def create_multiple_response(self, responses, name, alias, description='', notes=''):
        """
        Creates a Multiple response (array) using a set of rules for each
        of the responses(subvariables).
        """
        responses_map = collections.OrderedDict()
        responses_map_ids = []
        for resp in responses:
            case = resp['case']
            if isinstance(case, six.string_types):
                case = process_expr(parse_expr(case), self.resource)

            resp_id = '%04d' % resp['id']
            responses_map_ids.append(resp_id)
            responses_map[resp_id] = case_expr(
                case,
                name=resp['name'],
                alias='%s_%d' % (alias, resp['id'])
            )

        payload = {
            'element': 'shoji:entity',
            'body': {
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
                            {'value': responses_map_ids}
                        ]
                    }]
                }
            }
        }

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return an instance of Variable
        return self[new_var['body']['alias']]

    def copy_variable(self, variable, name, alias):
        _subvar_alias = re.compile(r'.+_(\d+)$')

        def subrefs(_variable, _alias):
            # In the case of MR variables, we want the copies' subvariables
            # to have their aliases in the same pattern and order that the
            # parent's are, that is `parent_alias_#`.
            _subreferences = []
            for subvar in _variable.resource.subvariables.index.values():
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

        if variable.resource.body.get('derivation'):
            # We are dealing with a derived variable, we want the derivation
            # to be executed again instead of doing a `copy_variable`
            derivation = abs_url(variable.resource.body['derivation'],
                                 variable.resource.self)
            derivation.pop('references', None)
            payload = {
                'element': 'shoji:entity',
                'body': {
                    'name': name,
                    'alias': alias,
                    'derivation': derivation
                }
            }

            if variable.type == _MR_TYPE:
                # We are re-executing a multiple_response derivation.
                # We need to update the complex `array` function expression
                # to contain the new suffixed aliases. Given that the map is
                # unordered, we have to iterated and find a name match.
                subvars = payload['body']['derivation']['args'][0]['args'][0]['map']
                subreferences = subrefs(variable, alias)
                for subref in subreferences:
                    for subvar_pos in subvars:
                        subvar = subvars[subvar_pos]
                        if subvar['references']['name'] == subref['name']:
                            subvar['references']['alias'] = subref['alias']
                            break
        else:
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
            if variable.type == _MR_TYPE:
                subreferences = subrefs(variable, alias)
                payload['body']['derivation']['references'] = {
                    'subreferences': subreferences
                }

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return an instance of Variable
        return self[new_var['body']['alias']]

    def combine_categories(self, variable, map, categories, missing=None, default=None,
                           name='', alias='', description=''):
        if not alias or not name:
            raise ValueError("Name and alias are required")
        if variable.type in _MR_TYPE:
            return self.combine_multiple_response(variable, map, categories, name=name,
                                                  alias=alias, description=description)
        else:
            return self.combine_categorical(variable, map, categories, missing, default,
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
        combinations = combinations_from_map(map, categories or {}, missing or [])
        payload = _VARIABLE_PAYLOAD_TMPL.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['derivation'] = combine_categories_expr(
            variable.resource.self, combinations)

        # this returns an entity
        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # at this point we are returning a Variable instance
        return self[new_var['body']['alias']]

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
        payload = _VARIABLE_PAYLOAD_TMPL.copy()
        payload['body']['name'] = name
        payload['body']['alias'] = alias
        payload['body']['description'] = description
        payload['body']['derivation'] = combine_responses_expr(
            variable.resource.self, responses)

        new_var = self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return an instance of Variable
        return self[new_var['body']['alias']]

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
            autorollback=autorollback
        )
        self.resource.actions.post(body)

    def delete_forks(self):
        """
        Deletes all the forks on the dataset. CANNOT BE UNDONE!
        """

        for fork in six.itervalues(self.resource.forks.index):
            fork.entity.delete()

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
        right_var_url = right_ds[right_var].url
        left_var_url = self[left_var].url
        # this dictionary sets the main part of the join
        adapter = {
            'function': 'adapt',
            'args': [
                {'dataset': right_ds.url},
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
                var_url = right_ds[var].url
                payload['body']['args'][0]['map'][var_url] = {'variable': var_url}

        if filter:
            # in the case of a filter, convert it to crunch
            # and attach the filter to the payload
            expr = process_expr(parse_expr(filter), right_ds)
            payload['body']['filter'] = {'expression': expr}

        progress = self.resource.variables.post(payload)
        # poll for progress to finish or return the url to progress
        if wait:
            return wait_progress(r=progress, session=self.resource.session, entity=self)
        return progress.json()['value']

    def create_numeric(self, alias, name, derivation, description='', notes=''):
        """
        Used to create new numeric variables using Crunchs's derived expressions
        """
        expr = parse_expr(derivation)

        if not hasattr(self.resource, 'variables'):
            self.resource.refresh()

        payload = dict(
            element='shoji:entity',
            body=dict(
                alias=alias,
                name=name,
                derivation=expr,
                description=description,
                notes=notes
            )
        )

        self.resource.variables.create(payload)
        # needed to update the variables collection
        self._reload_variables()
        # return the variable instance
        return self[alias]

    def create_categorical(self, categories, alias, name, multiple,
                           description='', notes=''):
        """
        Used to create new categorical variables using Crunchs's `case`
        function

        Will create either categorical variables or multiple response depending
        on the `multiple` parameter.
        """
        if multiple:
            return self.create_multiple_response(
                categories, alias=alias, name=name, description=description,
                notes=notes)
        else:
            return self.create_single_response(
                categories, alias=alias, name=name, description=description,
                notes=notes)

    def create_crunchbox(
            self, title='', header='', footer='', notes='',
            filters=None, variables=None, force=False, min_base_size=None,
            palette=None):
        """
        create a new boxdata entity for a CrunchBox.

        NOTE: new boxdata is only created when there is a new combination of
        where and filter data.

        Args:
            title       (str): Human friendly identifier
            notes       (str): Other information relevent for this CrunchBox
            header      (str): header information for the CrunchBox
            footer      (str): footer information for the CrunchBox
            filters    (list): list of filter names or `Filter` instances
            where      (list): list of variable aliases or `Variable` instances
                               If `None` all variables will be included.
            min_base_size (int): min sample size to display values in graph
            palette     dict : dict of colors as documented at docs.crunch.io
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

        if not title:
            title = 'CrunchBox for {}'.format(str(self))

        payload = dict(
            element='shoji:entity',
            body=dict(
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

    def compare_dataset(self, dataset):
        """
        compare the difference in structure between datasets. The
        criterion is the following:

        (1) variables that, when matched across datasets by alias, have different types.
        (2) variables that have the same name but don't match on alias.
        (3) for variables that match and have categories, any categories that have the
        same id but don't match on name.
        (4) for array variables that match, any subvariables that have the same name but
        don't match on alias.
        (5) array variables that, after assembling the union of their subvariables,
        point to subvariables that belong to other ds (Not implemented)

        :param: dataset: Daatset instance to append from
        :return: a dictionary of differences

        NOTE: this sould be done via: http://docs.crunch.io/#post217
        but doesn't seem to be a working feature of Crunch
        """
        diff = {
            'variables': {
                'by_type': [],
                'by_alias': []
            },
            'categories': {},
            'subvariables': {}
        }

        array_types = ['multiple_response', 'categorical_array']

        vars_a = {v.alias: v.type for v in self.values()}
        vars_b = {v.alias: v.type for v in dataset.values()}

        # 1. match variables by alias and compare types
        common_aliases = frozenset(vars_a.keys()) & frozenset(vars_b.keys())
        for alias in common_aliases:
            if vars_a[alias] != vars_b[alias]:
                diff['variables']['by_type'].append(dataset[alias].name)

            # 3. match variable alias and distcint categories names for same id's
            if vars_b[alias] == 'categorical' and vars_a[alias] == 'categorical':
                a_ids = frozenset([v.id for v in self[alias].categories.values()])
                b_ids = frozenset([v.id for v in dataset[alias].categories.values()])
                common_ids = a_ids & b_ids

                for id in common_ids:
                    a_name = self[alias].categories[id].name
                    b_name = dataset[alias].categories[id].name
                    if a_name != b_name:
                        if diff['categories'].get(dataset[alias].name):
                            diff['categories'][dataset[alias].name].append(id)
                        else:
                            diff['categories'][dataset[alias].name] = []
                            diff['categories'][dataset[alias].name].append(id)

        # 2. match variables by names and compare aliases
        common_names = frozenset(self.variable_names()) & frozenset(dataset.variable_names())
        for name in common_names:
            if self[name].alias != dataset[name].alias:
                diff['variables']['by_alias'].append(name)

            # 4. array types that match, subvars with same name and != alias
            if dataset[name].type == self[name].type and \
                self[name].type in array_types and \
                    self[name].type in array_types:

                a_names = frozenset(self[name].variable_names())
                b_names = frozenset(dataset[name].variable_names())
                common_subnames = a_names & b_names

                for sv_name in common_subnames:
                    if self[name][sv_name].alias != dataset[name][sv_name].alias:
                        if diff['subvariables'].get(name):
                            diff['subvariables'][name].append(dataset[name][sv_name].alias)
                        else:
                            diff['subvariables'][name] = []
                            diff['subvariables'][name].append(dataset[name][sv_name].alias)
        return diff

    def append_dataset(self, dataset, filter=None, where=None, autorollback=True):
        """
        Append dataset into self. If this operation fails, the
        append is rolledback. Dataset variables and subvariables
        are matched on their aliases and categories are matched by name.

        :param: dataset: Daatset instance to append from
        :param: filter: An expression to filter dataset rows. cannot be a Filter
            according to: http://docs.crunch.io/#get211
        :param: where: A list of variable names to include from dataset
        """
        if self.url == dataset.url:
            raise ValueError("Cannot append dataset to self")

        if where and not isinstance(where, list):
            raise AttributeError("where must be a list of variable names")

        payload = {
            "element": "shoji:entity",
            "autorollback": autorollback,
            "body": {
                'dataset': dataset.url}}

        if where:
            if isinstance(where, list):
                id_vars = []
                for var in where:
                    id_vars.append(dataset[var].url)
                # build the payload with selected variables
                payload['body']['where'] = {
                    'function': 'select',
                    'args': [{
                        'map': {
                            x: {'variable': x} for x in id_vars
                        }
                    }]
                }
            else:
                raise ValueError("where param must be a list of variable names")

        if filter:
            # parse the filter expression
            payload['body']['filter'] = process_expr(parse_expr(filter), dataset.resource)

        return self.resource.batches.create(payload)
