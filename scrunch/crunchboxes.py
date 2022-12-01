# coding: utf-8

from scrunch.datasets import Variable
from scrunch.subentity import Filter


class CrunchBox(object):
    """
    A CrunchBox representation of boxdata.

    an instance cannot mutate it's metadata directly since boxdata doesn't
    support PATCHing. Instead, simply create a new `CrunchBox` instance with
    the same Filters and Variables. You'll get the same entity from the boxdata
    index with the updated metadata.

    :param shoji_tuple: pycrunch.shoji.Tuple of boxdata
    :param     dataset: scrunch.datasets.BaseDataset instance

    NOTE: since the boxdata entity is different regarding the mapping of body
          and metadata fields, methods etc... it is made `readonly`.
          Since an `edit` method would need to return a new
          instance (see above) the `__setattr__` method ist incorporated with
          CrunchBox specific messages.

          (an edit method returning an instance would most likely brake user
          expectations)

          In order to have a proper `remove` method we also need the Dataset
          instance.
    """

    WIDGET_URL = 'https://s.crunch.io/widget/index.html#/ds/{id}/'
    DIMENSIONS = dict(height=480, width=600)

    # the attributes on entity.body.metadata
    _METADATA_ATTRIBUTES = {'title', 'notes', 'header', 'footer'}

    _MUTABLE_ATTRIBUTES = _METADATA_ATTRIBUTES

    _IMMUTABLE_ATTRIBUTES = {
        'id', 'user_id', 'creation_time', 'filters', 'variables'}

    # removed `dataset` from the set above since it overlaps with the Dataset
    # instance on self. `boxdata.dataset` simply points to the dataset url

    _ENTITY_ATTRIBUTES = _MUTABLE_ATTRIBUTES | _IMMUTABLE_ATTRIBUTES

    def __init__(self, shoji_tuple, dataset):
        self.resource = shoji_tuple
        self.url = shoji_tuple.entity_url
        self.dataset = dataset

    def __setattr__(self, attr, value):
        """ known attributes should be readonly """

        if attr in self._IMMUTABLE_ATTRIBUTES:
            raise AttributeError(
                "Can't edit attibute '%s'" % attr)
        if attr in self._MUTABLE_ATTRIBUTES:
            raise AttributeError(
                "Can't edit '%s' of a CrunchBox. Create a new one with "
                "the same filters and variables to update its metadata" % attr)
        object.__setattr__(self, attr, value)

    def __getattr__(self, attr):
        if attr in self._METADATA_ATTRIBUTES:
            return self.resource.metadata[attr]

        if attr == 'filters':
            # return a list of `Filters` instead of the filters expr on `body`
            _filters = []
            for obj in self.resource.filters:
                f_url = obj['filter']
                _filters.append(
                    Filter(self.dataset.resource.filters.index[f_url]))
            return _filters

        if attr == 'variables':
            # return a list of `Variables` instead of the where expr on `body`
            _var_urls = []
            _var_map = self.resource.where.args[0].map
            for v in _var_map:
                _var_urls.append(_var_map[v]['variable'])

            return [
                Variable(entity, self.dataset)
                for url, entity in self.dataset._vars
                if url in _var_urls
            ]

        # all other attributes not catched so far
        if attr in self._ENTITY_ATTRIBUTES:
            return self.resource[attr]
        raise AttributeError('CrunchBox has no attribute %s' % attr)

    def __repr__(self):
        return "<CrunchBox: title='{}'; id='{}'>".format(
            self.title, self.id)

    def __str__(self):
        return self.title

    def remove(self):
        self.dataset.resource.session.delete(self.url)

    @property
    def widget_url(self):
        return self.WIDGET_URL.format(id=self.id)

    @widget_url.setter
    def widget_url(self, _):
        """ prevent edits to the widget_url """
        raise AttributeError("Can't edit 'widget_url' of a CrunchBox")

    def iframe(self, logo=None, dimensions=None):
        dimensions = dimensions or self.DIMENSIONS
        widget_url = self.widget_url

        if not isinstance(dimensions, dict):
            raise TypeError('`dimensions` needs to be a dict')

        def _figure(html):
            return '<figure style="text-align:left;" class="content-list-'\
                   'component image">' + '  {}'.format(html) + \
                   '</figure>'

        _iframe = (
            '<iframe src="{widget_url}" width="{dimensions[width]}" '
            'height="{dimensions[height]}" style="border: 1px solid #d3d3d3;">'
            '</iframe>')

        if logo:
            _img = '<img src="{logo}" stype="height:auto; width:200px;'\
                   ' margin-left:-4px"></img>'
            _iframe = _figure(_img) + _iframe

        elif self.title:
            _div = '<div style="padding-bottom: 12px">'\
                   '    <span style="font-size: 18px; color: #444444;'\
                   ' line-height: 1;">' + self.title + '</span>'\
                   '  </div>'
            _iframe = _figure(_div) + _iframe

        return _iframe.format(**locals())