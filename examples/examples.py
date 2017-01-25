# coding: utf-8

from scrunch.helpers import subvar_alias

# These are the categories that multiple response use. Selected and Not Selected
MR_CATS = [
    {"id": 1, "name": "Selected", "missing": False, "numeric_value": None, "selected": True},
    {"id": 2, "name": "Not selected", "missing": False, "numeric_value": None, "selected": False}
]

NEWS_DATASET = {
    "caseid": {
        "name": "Case ID",
        "type": "numeric"
    },
    "age": {
        "name": "Age",
        "type": 'numeric',
    },
    "newssource": {
        "name": "News source",
        "type": "multiple_response",
        "categories": MR_CATS,
        "subreferences": [{
            "name": "Facebook",
            "alias": "newssource_1"
        }, {
            "name": "Twitter",
            "alias": "newssource_2"
        }, {
            "name": "Google news",
            "alias": "newssource_3"
        }, {
            "name": "Reddit",
            "alias": "newssource_4"
        }, {
            "name": "NY Times (Print)",
            "alias": "newssource_5"
        }, {
            "name": "Washington Post (Print)",
            "alias": "newssource_6"
        }, {
            "name": "NBC News",
            "alias": "newssource_7"
        }, {
            "name": "NPR",
            "alias": "newssource_8"
        }, {
            "name": "Fox",
            "alias": "newssource_9"
        }, {
            "name": "Local radio",
            "alias": "newssource_10"
        }]
    },
    "socialmedia": {
        "name": "Accounts in social media",
        "type": "multiple_response",
        "categories": MR_CATS,
        "subreferences": [{
            "name": "Facebook",
            "alias": "socialmedia_1"
        }, {
            "name": "Twitter",
            "alias": "socialmedia_2"
        }, {
            "name": "Google+",
            "alias": "socialmedia_3"
        }, {
            "name": "VK",
            "alias": "socialmedia_4"
        }]
    },
    "gender": {
        "name": "Gender",
        "type": "categorical",
        "categories": [
            {"id": 1, "name": "Female", "missing": False, "numeric_value": None},
            {"id": 2, "name": "Male", "missing": False, "numeric_value": None},
            {"id": -1, "name": "No Data", "missing": True, "numeric_value": None},
        ]
    }
}


NEWS_DATASET_ROWS = {
    "caseid": [1, 2, 3, 4, 5, 6, 7],
    "age": [25, 41, 33, 38, 50, 17, 61],
    "newssource_1": [1, 1, 1, 1, 2, 1, 2],
    "newssource_2": [2, 2, 1, 2, 2, 1, 2],
    "newssource_3": [1, 2, 1, 1, 2, 1, 2],
    "newssource_4": [1, 2, 1, 1, 2, 2, 2],
    "newssource_5": [2, 1, 2, 1, 1, 2, 2],
    "newssource_6": [2, 2, 1, 2, 1, 2, 2],
    "newssource_7": [2, 1, 1, 1, 2, 2, 2],
    "newssource_8": [2, 1, 1, 2, 2, 2, 2],
    "newssource_9": [2, 2, 2, 2, 1, 2, 2],
    "newssource_10": [2, 1, 2, 2, 1, 2, 1],
    "socialmedia_1": [1, 2, 1, 1, 2, 1, 2],
    "socialmedia_2": [1, 2, 1, 1, 2, 1, 2],
    "socialmedia_3": [2, 2, 1, 2, 2, 1, 2],
    "socialmedia_4": [2, 2, 1, 2, 2, 2, 2],
    "gender": [1, 2, 2, 1, 1, 1, 2]
}


def mr_in(ds, mr_alias, subvars):
    """
    Temporary helper until scrunch can parse correctly the expression:
     mr.has_any([sv1, sv2...])
    """
    variables = ds.resource.variables.by('alias')
    mr = variables[mr_alias].entity
    subvariables = mr.subvariables.by('alias')
    return {
        'function': 'any',
        'args': [{
            'variable': mr.self
        }, {
            'column': [subvariables[subvar_alias(mr_alias, sv)].id for sv in subvars]
        }]
    }
