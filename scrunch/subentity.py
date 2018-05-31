# coding: utf-8
import json

import six.moves.urllib as urllib
from six import PY2

from pycrunch.lemonpy import URL
from pycrunch.progress import DefaultProgressTracking
from pycrunch.shoji import wait_progress
from scrunch.helpers import download_file


class SubEntity:
    """
    A pycrunch.shoji.Entity directly related to a Dataset.
    For example; filters, decks
    """
    _MUTABLE_ATTRIBUTES = set()
    _IMMUTABLE_ATTRIBUTES = set()
    _ENTITY_ATTRIBUTES = set()

    def __init__(self, shoji_tuple):
        self.resource = shoji_tuple.entity

    def __getattr__(self, item):
        if item in self._ENTITY_ATTRIBUTES:
            return self.resource.body[item]
        raise AttributeError(
            '{} has no attribute {}'.format(self.__class__.__name__, item))

    def __repr__(self):
        if PY2:
            name = self.name.encode("ascii", "replace")
        else:
            name = self.name
        return u"<{}: name='{}'; id='{}'>".format(
            self.__class__.__name__, name, self.id)

    def __str__(self):
        return self.__repr__()

    def edit(self, **kwargs):
        for key in kwargs:
            if key not in self._MUTABLE_ATTRIBUTES:
                raise AttributeError(
                    "Can't edit attribute {} of {}".format(key, self.name))
        return self.resource.edit(**kwargs)

    def remove(self):
        self.resource.delete()

    def delete(self):
        """
        Aalias to remove method
        """
        self.remove()


class Filter(SubEntity):
    """
    A pycrunch.shoji.Entity for Dataset filters
    """
    _MUTABLE_ATTRIBUTES = {'name', 'template', 'is_public', 'owner_id'}
    _IMMUTABLE_ATTRIBUTES = {'id', }
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES


class Multitable(SubEntity):
    """
    A pycrunch.shoji.Entity for Multitables
    """
    _MUTABLE_ATTRIBUTES = {'name', 'template', 'is_public'}
    _IMMUTABLE_ATTRIBUTES = {'id', }
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, shoji_tuple, ds):
        self.resource = shoji_tuple.entity
        # a dataset instance to make things simpler
        self.ds = ds

    def query_cube(self):
        """
        http://docs.crunch.io/#more-complex-multitable-templates
        """
        raise NotImplementedError

    def export_tabbook(self, format, progress_tracker=None, filter=None,
                       where=None, options=None, weight=False):
        """
        An adaption of https://github.com/Crunch-io/pycrunch/blob/master/pycrunch/exporting.py
        to Multitables exports (tabbboks)
        """
        payload = {}

        # add filter to multitable
        if filter:
            if isinstance(filter, Filter):
                payload['filter'] = [{'filter': filter.resource.self}]
            else:
                raise ValueError('filter param must be a Filter instance')

        if options and isinstance(options, dict):
            payload['options'] = options

        if where:
            if isinstance(where, list):
                id_vars = []
                for var in where:
                    id_vars.append(self.ds[var].url)
                # Now build the payload with selected variables
                payload['where'] = {
                    'function': 'select',
                    'args': [{
                        'map': {
                            x: {'variable': x} for x in id_vars
                        }
                    }]
                }
            else:
                raise ValueError('where param must be a list of variable names')

        if weight:
            payload['weight'] = self.ds[weight].url
        if weight is None:
            payload['weight'] = None

        session = self.resource.session
        endpoint = self.resource.views['tabbook']

        # in case of json format, we need to return the json response
        if format == 'json':
            r = session.post(
                endpoint,
                json.dumps(payload),
                headers={'Accept': 'application/json'})
        else:
            r = session.post(endpoint, json.dumps(payload))
        dest_file = URL(r.headers['Location'], '')
        if r.status_code == 202:
            try:
                r.payload['value']
            except Exception:
                # Not a progress API just return the incomplete entity.
                # User will refresh it.
                pass
            else:
                # We have a progress_url, wait for completion
                wait_progress(r, session, progress_tracker)
        return dest_file

    def export(self, path, format='xlsx', timeout=None, filter=None,
               where=None, options=None, **kwargs):
        """
        A tabbook export: http://docs.crunch.io/#tab-books
        Exports data as csv to the given path or as a JSON response
        :params: http://docs.crunch.io/#post-body-parameters
        :path: Local Filesystem path to save the file to
        :filter: Name of Filter instance of a Dataset's filter
        :where: list of variables to include; ['varA', 'varB']
        :options: Display options as python dictionary
        :weight: Name of the weight_variable
        """
        if format not in ['xlsx', 'json']:
            raise ValueError("Format can only be 'json' or 'xlxs'")
        progress_tracker = DefaultProgressTracking(timeout)
        tabbook_args = dict(
            format=format,
            progress_tracker=progress_tracker,
            filter=filter,
            where=where,
            options=options,
        )
        if 'weight' in kwargs:
            tabbook_args['weight'] = kwargs['weight']
        else:
            tabbook_args['weight'] = False
        url = self.export_tabbook(**tabbook_args)
        download_file(url, path)


class Deck(SubEntity):
    """
    A pycrunch.shoji.Entity for Dataset decks
    """
    _MUTABLE_ATTRIBUTES = {'name', 'description', 'is_public',
                           'owner_id', 'owner_name'}
    _IMMUTABLE_ATTRIBUTES = {'id', 'creation_time', 'slides'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __repr__(self):
        return "<{}: id='{}'; name='{}'>".format(
            self.__class__.__name__, self.id, self.name)

    @property
    def slides(self):
        _slides = {}
        for slide in self.resource.slides.index.values():
            slide_inst = Slide(slide)
            _slides[slide_inst.id] = slide_inst
        return _slides

    @slides.setter
    def slides(self, _):
        # Protect `slides` property from direct modifications.
        raise TypeError('Use add_decks method to add a new deck')

    def xlsx_export(self):
        raise NotImplementedError


class Slide(SubEntity):
    """
    A pycrunch.shoji.Entity for a Slide
    """
    _MUTABLE_ATTRIBUTES = {'display_settings', 'analysis_url', 'title',
                           'subtitle', 'deck_id'}
    _IMMUTABLE_ATTRIBUTES = {'id', 'dataset_id', 'version', 'analyses'}
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __repr__(self):
        return "<{}: id='{}'; title='{}'>".format(
            self.__class__.__name__, self.id, self.title)

    @property
    def analyses(self):
        _analyses = {}
        for url, a in self.resource.analyses.index.items():
            id = url.split('/')[-2]
            _analyses[id] = Analysis(a, id)
        return _analyses

    @analyses.setter
    def analyses(self, _):
        # Protect `analyses` property from direct modifications.
        raise TypeError('Use add_decks method to add a new deck')


class Analysis:
    """
    A cube query JSON representation and transcriber
    """

    def __init__(self, shoji_tuple, id):
        self.resource = shoji_tuple.entity
        self.id = id

    def __getitem__(self, item):
        try:
            return self.resource.body[item]
        except AttributeError:
            raise AttributeError(
                '{} has no attribute {}'.format(
                    self.__class__.__name__, item))

    def __repr__(self):
        return "<{}: id='{}'>".format(self.__class__.__name__, self.id)

    def query_cube(self, ds):
        """
        :param: ds: Dataset() instance
        out of the current instance GET a cube query
        :return: a shoji:view json instance
        """
        json_string = self['query'].json
        # this process removes newlines
        dict_obj = json.loads(json_string)
        resp = ds.resource.follow(
            'cube',
            urllib.parse.urlencode({'query': json.dumps(dict_obj)})
        )
        return resp.json

    def as_dataframe(self):
        """
        It would be awesome to visualize a table or Analysis
        as a pandas.Dataframe or even Numpy Array
        """
        raise NotImplementedError
