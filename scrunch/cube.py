import json


def get_cube(ds, dimensions=[], measures=None, filter=[], weight=None):
    """
    @param dimensions: list of dicts:
                variable (alias),
        TODO    a function over it,
        TODO    a traversal of its subvariables
        TODO    a combination of multiple variables (A + B)
                see http://docs.crunch.io/#dimensions
    @param measures: dict of named functions,
                     defaults to 'cube_count'
        i.e.:
        {"count": {"function": "cube_count", "args": []}}}

        {
            "mean": {
                "function": "cube_mean",
                "args": [
                    {"variable": "var.url"}
                ]
            }
        }

        {
            "stddev": {
                "function": "cube_stddev",
                "args": [
                    {"variable": "var.url"}
                ]
            }
        }

        {
            "mean": {
                "function": "cube_mean",
                "args": [{
                    "function": "cast",
                    "args": [
                        {"variable": "categorical_var.url"},
                        {"class": "numeric"}
                    ]
                }]
            }
        }
    """
    # MEASURES_MAP = dict(
    #     count='cube_count',
    #     mean='cube_mean',
    #     stddev='cube_stddev'
    # )

    _dimensions = []
    for d in dimensions:
        var = d.get('variable')
        if var:
            var = ds[var]  # alias is expected
            subvars = var.shoji_tuple.get('subvariables')
            if subvars:
                _dimensions.append({'each': var.url})
                if var.type == 'multiple_response':
                    _dimensions.append({
                        'function': 'selections',
                        'args': [
                            {'variable': var.url}
                        ]
                    })

            if var.type in ('categorical', 'categorical_array', 'text'):
                _dimensions.append({'variable': var.url})

        func = d.get('function')
        if func:
            # TODO: for now, just append it as passed. var.url needed here!
            # i.e.:
            # - text: {'function': 'bin', 'args': [{'variable': url}]}
            # - datetime:
            #       {
            #           'function': 'rollup',
            #           'args': [
            #               {'variable': url},
            #               {'value': variable.rollup_resolution}
            #           ]
            #       }

            _dimensions.append(d)

    # TODO: simplify the usage of measures
    # perhaps a list of dicts like:
    # [{'count': 'var.url'}, {'mean': 'var.url'}]
    # which uses MEASURES_MAP and expands 'mean' in case of a categorical
    _measures = measures or {
        'count': {
            'function': 'cube_count',
            'args': []
        }
    }

    payload = dict(
        dimensions=_dimensions,
        measures=_measures,
        weight=weight
    )
    url = ds.url + 'cube/'
    query = '?filter=%s&query=%s' % (json.dumps(filter),
                                     json.dumps(payload))
    return ds.resource.session.get(url + query).json()
