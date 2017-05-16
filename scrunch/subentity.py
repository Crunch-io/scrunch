import six.moves.urllib as urllib
import json


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
        return "<{}: name='{}'; id='{}'>".format(
            self.__class__.__name__, self.name, self.id)

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


class Filter(SubEntity):
    """
    A pycrunch.shoji.Entity for Dataset filters
    """
    _MUTABLE_ATTRIBUTES = {'name', 'expression', 'is_public', 'owner_id'}
    _IMMUTABLE_ATTRIBUTES = {'id', }
    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES


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
