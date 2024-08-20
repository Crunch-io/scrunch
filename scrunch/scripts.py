# coding: utf-8

import json
import pycrunch
from pycrunch.shoji import TaskError

from scrunch.helpers import shoji_view_wrapper, shoji_entity_wrapper


class ScriptExecutionError(Exception):
    def __init__(self, client_error, resolutions):
        self.client_error = client_error
        self.resolutions = resolutions

    def __repr__(self):
        return json.dumps(self.resolutions, indent=2)


DEFAULT_SUBVARIABLE_SYNTAX = False


class BaseScript:
    def __init__(self, resource):
        """
        :param resource: Pycrunch Entity.
        """
        self.resource = resource

    def get_default_syntax_flag(self, strict_subvariable_syntax):
        """
        The Syntax flag tells scripts whether to be strict about
        evaluating subvariable codes with the array[subvar] square bracket
        syntax (when True) or to allow referring to subvariables by their
        global code only  (when False).

        If the user did provide a flag (not None), honor it and send that
        in the request.

        IFF the user did NOT provide a flag (is None), then see if the
        session's feature flags know something that's been provided from the
        server, and if nothing's there use the constant's default (currently
        False).

        :param strict_subvariable_syntax: User provided flag
        :return: bool
        """
        if strict_subvariable_syntax is not None:
            return strict_subvariable_syntax
        flags = self.resource.session.feature_flags
        return flags.get(
            "clients_strict_subvariable_syntax", DEFAULT_SUBVARIABLE_SYNTAX
        )

    def execute(self, script_body, strict_subvariable_syntax=None):
        pass


class SystemScript(BaseScript):
    def format_request_url(self, request_url, strict_subvariable_syntax=None):
        strict_subvariable_syntax_flag = self.get_default_syntax_flag(
            strict_subvariable_syntax
        )
        if strict_subvariable_syntax_flag:
            request_url += "?strict_subvariable_syntax=true"
        return request_url

    def execute(self, script_body, strict_subvariable_syntax=None):
        """
        Will run a system script on this.

        System scripts do not have a return value. If they execute correctly
        they'll finish silently. Otherwise, an error will raise.
        """
        # The script execution endpoint is a shoji:view
        payload = shoji_view_wrapper(script_body)
        try:
            execute_url = self.format_request_url(
                self.resource.views["execute"], strict_subvariable_syntax
            )
            return self.resource.session.post(execute_url, json=payload)
        except pycrunch.ClientError as err:
            resolutions = err.args[2]["resolutions"]
            raise ScriptExecutionError(err, resolutions)


class DatasetScripts(BaseScript):
    def execute(self, script_body, strict_subvariable_syntax=None, dry_run=False):
        strict_subvariable_syntax = self.get_default_syntax_flag(
            strict_subvariable_syntax
        )
        payload = {
            "body": script_body,
            "strict_subvariable_syntax": strict_subvariable_syntax,
        }

        if dry_run:
            payload["dry_run"] = True
            method = self.resource.scripts.post
        else:
            method = self.resource.scripts.create

        # The dataset script execution endpoint is a shoji:entity
        body = shoji_entity_wrapper(payload)

        try:
            method(body)
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

    def dry_run(self, script_body, strict_subvariable_syntax=None):
        self.execute(script_body, strict_subvariable_syntax, dry_run=True)

    def collapse(self):
        """
        When a dataset has too many scripts. Collapse will concatenate
        all the previously executed scripts into one the first. It will delete
        all savepoints associated with the collapsed scripts.
        """
        self.resource.scripts.collapse.post({})

    def all(self):
        scripts_index = self.resource.scripts.index
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
        pycrunch.shoji.wait_progress(resp, self.resource.session)
