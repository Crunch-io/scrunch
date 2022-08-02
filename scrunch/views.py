# coding: utf-8

from scrunch.helpers import shoji_entity_wrapper


class DatasetViews:
    def __init__(self, dataset_resource):
        self.dataset_resource = dataset_resource

    def create(self, name, columns):
        """
        Creates a new View on this dataset

        :param name: Name of the view
        :param columns: Use None to copy all views, otherwise a list of aliases
        :return: Mutable Dataset for the new view
        """
        view_args = {
            "name": name,
            "view_of": self.dataset_resource.self,
            "owner": self.dataset_resource.body["owner"]
        }
        if columns is not None:
            # Columns is a list of aliases, convert to URLs
            alias_2_url = self.dataset_resource.variables.by("alias")
            columns_url = [alias_2_url[a].entity_url for a in columns]
            view_args["view_cols"] = columns_url

        project = self.dataset_resource.session.root.datasets
        view_res = project.create(shoji_entity_wrapper(view_args))
        view_res.refresh()

        from scrunch.mutable_dataset import MutableDataset
        return MutableDataset(view_res)

    def get_by_name(self, view_name):
        """
        Returns a view by name as a MutableDataset
        """
        from scrunch.mutable_dataset import MutableDataset
        views_by_name = self.dataset_resource.follow("views").by("name")
        view_res = views_by_name[view_name].entity
        return MutableDataset(view_res)

    def all_names(self):
        """
        Returns a list of the names of available views on this dataset.
        """
        views_index = self.dataset_resource.follow("views").index
        return {t["name"] for t in views_index.values()}
