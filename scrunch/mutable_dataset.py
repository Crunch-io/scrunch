from pycrunch.shoji import wait_progress

from scrunch.datasets import BaseDataset, _get_connection, _get_dataset
from scrunch.exceptions import (
    InvalidDatasetTypeError,
    InvalidVariableTypeError,
    InvalidParamError
)
from scrunch.expressions import parse_expr, process_expr
from scrunch.helpers import shoji_entity_wrapper


def get_mutable_dataset(dataset, connection=None, editor=False, project=None):
    """
    A simple wrapper of _get_dataset with streaming=False
    """
    shoji_ds, root = _get_dataset(dataset, connection, editor, project)
    # make sure the Dataset is of type streaming != "streaming"
    if shoji_ds['body'].get('streaming') == 'streaming':
        raise InvalidDatasetTypeError("Dataset %s is of type 'streaming',\
            use get_streaming_dataset method instead" % dataset)
    ds = MutableDataset(shoji_ds)
    if editor is True:
        ds.change_editor(root.session.email)
    return ds


def create_dataset(name, variables, connection=None, **kwargs):
    if connection is None:
        connection = _get_connection()
        if not connection:
            raise AttributeError(
                "Authenticate first with scrunch.connect() or by providing "
                "config/environment variables")

    dataset_doc = {
        'name': name,
        'table': {
            'element': 'crunch:table',
            'metadata': variables
        }
    }
    dataset_doc.update(**kwargs)

    shoji_ds = connection.datasets.create(shoji_entity_wrapper(dataset_doc)).refresh()
    return MutableDataset(shoji_ds)


class MutableDataset(BaseDataset):
    """
    Class that enclose mutable dataset methods or any
    method that varies the state of the dataset and/or it's data.
    """
    def delete(self):
        """
        Delete a dataset.
        """
        self.resource.delete()

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
        payload = shoji_entity_wrapper(adapter)

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

    def append_dataset(
            self, dataset, filter=None, variables=None, autorollback=True):
        """
        Append dataset into self. If this operation fails, the
        append is rolledback. Dataset variables and subvariables
        are matched on their aliases and categories are matched by name.

        :param: dataset: Daatset instance to append from
        :param: filter: An expression to filter dataset rows. cannot be a Filter
            according to: http://docs.crunch.io/#get211
        :param: variables: A list of variable names to include from dataset
        """
        if self.url == dataset.url:
            raise ValueError("Cannot append dataset to self")

        if variables and not isinstance(variables, list):
            raise AttributeError("'variables' must be a list of variable names")

        payload = shoji_entity_wrapper({'dataset': dataset.url})
        payload['autorollback'] = autorollback

        if variables:
            id_vars = []
            for var in variables:
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

        if filter:
            # parse the filter expression
            payload['body']['filter'] = process_expr(parse_expr(filter), dataset.resource)

        return self.resource.batches.create(payload)

    def _validate_vartypes(self, var_type, resolution=None, subvariables=None,
        categories=None):
        if var_type not in ('text', 'numeric', 'categorical', 'datetime',
            'multiple_response', 'categorical_array'):
            raise InvalidVariableTypeError

        resolution_types = ('Y', 'M', 'D', 'h', 'm', 's', 'ms')
        if var_type == 'datetime' and resolution not in resolution_types:
            raise InvalidParamError(
                'Include a valid resolution parameter when creating \
                datetime variables. %s' % resolution_types)

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
                {'name' 'Subvariable 1', 'alias': 'subvar1', 'id': 1},
                {'name' 'Subvariable 2', 'alias': 'subvar2', 'id': 2}
            ]
        :param: categories: List of categories in the form:
            categories = [
                {'name': 'ManU', 'id': 1, 'numeric_value': 1, 'missing': False},
                {'name': 'Chelsea', 'id': 2, 'numeric_value': 2, 'missing': False},
                {'name': 'Totthenham', 'id': 3, 'numeric_value': 3, 'missing': False}
            ]
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
        if categories:
            payload['categories'] = categories
        if subvariables:
            payload['subreferences'] = [
                {'name': item['name']} for item in subvariables
            ]
        if values:
            payload['values'] = values
        self.resource.variables.create(shoji_entity_wrapper(payload))
        self._reload_variables()
        return self[name]
