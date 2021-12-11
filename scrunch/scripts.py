# coding: utf-8

import json
import pycrunch
from pycrunch.shoji import TaskError


class ScriptExecutionError(Exception):
    def __init__(self, client_error, resolutions):
        self.client_error = client_error
        self.resolutions = resolutions

    def __repr__(self):
        return json.dumps(self.resolutions, indent=2)


class DatasetScripts:
    def __init__(self, dataset_resource):
        """
        :param dataset_resource: Pycrunch Entity for the dataset.
        """
        self.dataset_resource = dataset_resource

    def execute(self, script_body):
        try:
            self.dataset_resource.scripts.create({
                'element': 'shoji:entity',
                'body': {"body": script_body},
            })
        except pycrunch.ClientError as err:
            if isinstance(err, TaskError):
                # For async script validation error
                resolutions = err.message["resolutions"]
                raise ScriptExecutionError(err, resolutions)
            elif err.status_code == 400:
                # For sync validation
                resolutions = err.args[2]["resolutions"]
                raise ScriptExecutionError(err, resolutions)
            raise err  # 404 or something else

    def collapse(self):
        """
        When a dataset has too many scripts. Collapse will concatenate
        all the previously executed scripts into one the first. It will delete
        all savepoints associated with the collapsed scripts.
        """
        self.dataset_resource.scripts.collapse.post({})

    def all(self):
        scripts_index = self.dataset_resource.scripts.index
        scripts = []
        for s_url, s in scripts_index.items():
            scripts.append(s.entity)
        scripts = sorted(scripts, key=lambda s: s.body["creation_time"])
        return scripts

    def revert_to(self, id=None, script_number=None):
        all_scripts = self.all()
        if script_number is not None:
            script = all_scripts[script_number]
        elif id is not None:
            # We have to do this because currently the API does not expose the
            # script ID directly.
            script = [s for s in all_scripts if "scripts/{}/".format(id) in s.self][0]
        else:
            raise ValueError("Must indicate either ID or script number")

        resp = script.revert.post({})  # Asynchronous request
        pycrunch.shoji.wait_progress(resp, self.dataset_resource.session)
