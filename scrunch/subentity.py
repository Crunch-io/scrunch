

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

    def add_slide(self, query, title='', subtitle=''):
        """
        :param analysis: A list of analysis (cube queries)
        """
        payload = {
            "element": "shoji:entity",
            "body": {
                "title": title,
                "subtitle": subtitle,
                "analyses": Analysis.build_analyses(query)  # TODO
            }
        }
        new_slide = self.resource.slides.create(payload)
        return self.slides[new_slide.body['id']]

    def xlsx_export(self):
        # TODO
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

    def add_analysis(self, query):
        """
        :param query: A cube query
        """
        # TODO
        raise NotImplementedError

    def as_dataframe(self):
        """
        Tries to generate a Dataframe from a Slide
        """
        # NOTE: Currently Slides only hold 1 analysis,
        # this shouldn't be the case in the future
        # TODO
        raise NotImplementedError


class Analysis:
    """
    A cube query JSON representation and transcriber
    """

    measures = {}
    dimensions = []
    weight = None
    query_environment = {}

    def __init__(self, shoji_tuple, id):
        self.resource = shoji_tuple.entity
        self.id = id

    def __repr__(self):
        return "<{}: id='{}'>".format(self.__class__.__name__, self.id)

    def set_query_environment(self, q_env):
        self.query_environment = q_env

    def build_analyses(self, query):
        """
        Puts all the structure together
        :param query: A nice, human formatted lies of queries
        :return: A list of analysis:
            [
                {
                  "query": {},
                  "query_environment": {},
                  "display_settings": {}
                },
                {
                  "query": {},
                  "query_environment": {},
                  "display_settings": {}
                }
            ]
        """
        raise NotImplementedError

    def build_query(self, dimensions, measures, weight):
        """
        {
            "measures": {
                "count": {
                    "function": "cube_count",
                    "args": []
                }
            },
            "dimensions": [
                {"variable": "<var_url>"},
                {"variable": "<var_url>"}
            ],
            "weight": null
        },
        :param dimensions: a list of variable names to include in the query
        :param measures: What crunch measures to apply
        """
        structure = {
            "measures": {},
            "dimensions": [],
            "weight": weight
        }
        return structure

    def build_display_settings(self):
        # Note: this seems to be a default
        # TODO: need to look into more detail
        return {
            "percentageDirection": {
                "value": "colPct"
            },
            "showEmpty": {
                "value": False
            },
            "slicesOrGroups": {
                "value": "groups"
            },
            "vizType": {
                "value": "table"
            },
            "countsOrPercents": {
                "value": "count"
            },
            "decimalPlaces": {
                "value": 0
            },
            "showSignif": {
                "value": False
            },
            "currentTab": {
                "value": 0
            },
            "uiView": {
                "value": "app.datasets.analyze"
            }
        }

    def as_dataframe(self):
        """
        It would be awesome to visualize a table or Analysis as a Dataframe
        """
        # TODO
        raise NotImplementedError
