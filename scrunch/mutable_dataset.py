import json

from pycrunch.shoji import wait_progress
from pycrunch.progress import DefaultProgressTracking
from scrunch.datasets import (LOG, BaseDataset, _get_connection, _get_dataset,
                              CATEGORICAL_TYPES)
from scrunch.exceptions import InvalidDatasetTypeError
from scrunch.expressions import parse_expr, process_expr
from scrunch.helpers import shoji_entity_wrapper

ARRAY_TYPES = frozenset(('multiple_response', 'categorical_array'))

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
        authenticated_url = root.urls["user_url"]
        ds.change_editor(authenticated_url)
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
             filter=None, timeout=30):
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
                'function': 'make_frame',
                'args': [
                    {'map': {}}
                ]
            }
            # add the individual variable columns to the payload
            alias_list = right_ds.resource.variables.by("alias")
            var_urls = [alias_list[alias].entity_url for alias in columns]
            var_url_list = {var_url: {"variable": var_url} for var_url in var_urls}
            payload['body']['args'][0]['map'] = var_url_list

        if filter:
            # in the case of a filter, convert it to crunch
            # and attach the filter to the payload
            expr = process_expr(parse_expr(filter), right_ds)
            payload['body']['filter'] = {'expression': expr}

        progress = self.resource.variables.post(payload)
        # poll for progress to finish or return the url to progress
        progress_tracker = DefaultProgressTracking(timeout)
        return wait_progress(r=progress, session=self.resource.session, progress_tracker=progress_tracker, entity=self)

    def compare_dataset(self, dataset, use_crunch=False):
        """
        Compare the difference in structure between datasets.

        The criterion is the following:
        (1) variables that, when matched across datasets by alias, have different types.
        (2) variables that have the same name but don't match on alias.
        (3) for variables that match and have categories, any categories that have the
        same id but don't match on name.
        (4) for array variables that match, any subvariables that have the same name but
        don't match on alias.
        (5) array variables that, after assembling the union of their subvariables,
        point to subvariables that belong to other ds (Not implemented)
        (6) missing rules of the variable.

        :param: dataset: Dataset instance to compare with
        :param: use_crunch: Use the Crunch comparison to compare
        :return: a dictionary of differences

        NOTE: this sould be done via: http://docs.crunch.io/#post217
        but doesn't seem to be a working feature of Crunch
        """

        if use_crunch:
            return self.resource.batches.follow('compare', 'dataset={}'.format(dataset.url))

        def process_metadata(metadata):
            """ Extract & format metadata with required information. """
            return {
                v["alias"]: {
                    "alias": v["alias"],
                    "name": v["name"],
                    "type": v["type"],
                    "categories": v.get("categories", [])
                    if v["type"] in CATEGORICAL_TYPES
                    else [],
                    "subvariables": v.get("subreferences", [])
                    if v["type"] in ARRAY_TYPES
                    else [],
                    "missing_rules": {
                        k: v['args'][1]['value']
                        for k, v in v.get('missing_rules', {}).items()
                    }
                }
                for v in metadata.values()
            }

        self_meta = process_metadata(self.resource.table["metadata"])
        dataset_meta = process_metadata(dataset.resource.table["metadata"])
        common_aliases = frozenset(self_meta.keys()) & frozenset(dataset_meta.keys())

        self_names = {}
        dataset_names = {}

        for n in self_meta.values():
            name = n["name"]
            alias = n["alias"]
            if self_names.get(name):
                self_names[name].append(alias)
            else:
                self_names[name] = [alias]

        for n in dataset_meta.values():
            name = n["name"]
            alias = n["alias"]
            if dataset_names.get(name):
                dataset_names[name].append(alias)
            else:
                dataset_names[name] = [alias]

        common_names = frozenset(self_names.keys()) & frozenset(dataset_names.keys())

        diff = {
            "variables": {"by_type": [], "by_alias": [], "by_missing_rules": []},
            "categories": {},
            "subvariables": {},
        }

        # 1. Compare types and categories by alias
        for alias in common_aliases:
            self_var, dataset_var = self_meta[alias], dataset_meta[alias]

            if self_var["type"] != dataset_var["type"]:
                diff["variables"]["by_type"].append(dataset_var["name"])

            # 3. Compare category names for categorical variables
            if self_var["type"] == dataset_var["type"] == "categorical":
                a_ids = {v["id"]: v["name"] for v in self_var["categories"]}
                b_ids = {v["id"]: v["name"] for v in dataset_var["categories"]}

                mismatched_cats = [
                    cat_id
                    for cat_id in (a_ids.keys() & b_ids.keys())
                    if a_ids[cat_id] != b_ids[cat_id]
                ]
                if mismatched_cats:
                    diff["categories"][alias] = mismatched_cats

        # 2. Compare aliases, subvariables, and missing rules by name
        for name in common_names:
            self_alias = set(self_names[name])
            dataset_alias = set(dataset_names[name])

            alias_diff = list(self_alias - dataset_alias)
            alias_common = set(self_alias & dataset_alias)

            if alias_diff:
                diff["variables"]["by_alias"].append(name)

            # 4. Compare subvariables for array types
            for com_als in alias_common:
                self_var, dataset_var = self_meta[com_als], dataset_meta[com_als]

                if self_var["type"] == dataset_var["type"] and self_var["type"] in ARRAY_TYPES:
                    a_names = {i["name"]: i["alias"] for i in self_var["subvariables"].values()}
                    b_names = {
                        i["name"]: i["alias"] for i in dataset_var["subvariables"].values()
                    }

                    mismatched_subs = [
                        b_names[sv_name]
                        for sv_name in (frozenset(a_names.keys()) & frozenset(b_names.keys()))
                        if a_names[sv_name] != b_names[sv_name]
                    ]
                    if mismatched_subs:
                        diff["subvariables"][name] = mismatched_subs

                # 6. Compare missing rules for non-categorical types
                if (
                    self_var["type"] not in CATEGORICAL_TYPES
                    and dataset_var["type"] not in CATEGORICAL_TYPES
                ):
                    if self_var["missing_rules"] != dataset_var["missing_rules"]:
                        diff["variables"]["by_missing_rules"].append(name)

        return diff

    def append_dataset(self, dataset, filter=None, variables=None,
                       autorollback=True, delete_pk=True):
        """ Append dataset into self. If this operation fails, the
        append is rolledback. Dataset variables and subvariables
        are matched on their aliases and categories are matched by name.

        :param: dataset: Dataset instance to append from
        :param: filter: An expression to filter dataset rows. cannot be a Filter
            according to: http://docs.crunch.io/#get211
        :param: variables: A list of variable names to include from dataset
        """
        if self.url == dataset.url:
            raise ValueError("Cannot append dataset to self")

        if variables and not isinstance(variables, list):
            raise AttributeError("'variables' must be a list of variable names")

        if delete_pk:
            LOG.info("Any pk's found will be deleted, to avoid these pass delete_pk=False")
            self.resource.pk.delete()
            dataset.resource.pk.delete()

        payload = shoji_entity_wrapper({'dataset': dataset.url})
        payload['autorollback'] = autorollback

        if variables:
            # This contains a list of variable IDs, not URLs
            id_vars = [dataset[var].id for var in variables]
            # build the payload with selected variables
            payload['body']['where'] = {
                'function': 'frame_subset',
                "args": [
                    {"frame": "primary"},
                    {"value": id_vars},
                ],
            }

        if filter:
            # parse the filter expression
            payload['body']['filter'] = process_expr(parse_expr(filter), dataset.resource)

        return self.resource.batches.create(payload)

    def move_to_categorical_array(
            self, name, alias, subvariables, description='', notes=''):
        """
        This is a dangerous method that allows moving variables (effectively
        translating them as variables in a dataset) as subvariables in the
        newly created categorical_array created.

        :param: name: Name of the new variable.
        :param: alias: Alias of the new variable
        :param: subvariables: A list of existing Dataset variables aliases
            to move into the new variable as subvariables .i.e;
                subvariables = ['var1_alias', 'var2_alias']
        :param: description: A description of the new variable
        :param: notes: Notes to attach to the new variable
        """
        payload = {
            'name': name,
            'alias': alias,
            'description': description,
            'notes': notes,
            'type': 'categorical_array',
            'subvariables': [self[v].url for v in subvariables]
        }
        self.resource.variables.create(shoji_entity_wrapper(payload))
        self._reload_variables()
        return self[alias]

    def move_to_multiple_response(
            self, name, alias, subvariables, description='', notes=''):
        """
        This method is a replication of the method move_to_categorical_array,
        only this time we are creting a multiple_response variable.
        Note: the subvariables need to have at least 1 selected catagory.
        """
        payload = {
            'name': name,
            'alias': alias,
            'description': description,
            'notes': notes,
            'type': 'multiple_response',
            'subvariables': [self[v].url for v in subvariables]
        }
        self.resource.variables.create(shoji_entity_wrapper(payload))
        self._reload_variables()
        return self[alias]

    def move_as_subvariable(self, destination, source):
        """
        Moves a variable as a subvariable of an existing array
        type variable.

        :param: destination: The alias of the variable that will receive the subvariable
        :param: source: Alias of the variable to move into destination as subvariable
        """
        payload = json.dumps({"element": "shoji:catalog", "index": {self[source].url: {}}})
        self[destination].resource.subvariables.patch(payload)
