# coding: utf-8

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
            "alias": "newssource-1"
        }, {
            "name": "Twitter",
            "alias": "newssource-2"
        }, {
            "name": "Google news",
            "alias": "newssource-3"
        }, {
            "name": "Reddit",
            "alias": "newssource-4"
        }, {
            "name": "NY Times (Print)",
            "alias": "newssource-5"
        }, {
            "name": "Washington Post (Print)",
            "alias": "newssource-6"
        }, {
            "name": "NBC News",
            "alias": "newssource-7"
        }, {
            "name": "NPR",
            "alias": "newssource-8"
        }, {
            "name": "Fox",
            "alias": "newssource-9"
        }, {
            "name": "Local radio",
            "alias": "newssource-10"
        }]
    },
    "socialmedia": {
        "name": "Accounts in social media",
        "type": "multiple_response",
        "categories": MR_CATS,
        "subreferences": [{
            "name": "Facebook",
            "alias": "socialmedia-1"
        }, {
            "name": "Twitter",
            "alias": "socialmedia-2"
        }, {
            "name": "Google+",
            "alias": "socialmedia-3"
        }, {
            "name": "VK",
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
    "newssource-1": [1, 1, 1, 1, 2, 1, 2],
    "newssource-2": [2, 2, 1, 2, 2, 1, 2],
    "newssource-3": [1, 2, 1, 1, 2, 1, 2],
    "newssource-4": [1, 2, 1, 1, 2, 2, 2],
    "newssource-5": [2, 1, 2, 1, 1, 2, 2],
    "newssource-6": [2, 2, 1, 2, 1, 2, 2],
    "newssource-7": [2, 1, 1, 1, 2, 2, 2],
    "newssource-8": [2, 1, 1, 2, 2, 2, 2],
    "newssource-9": [2, 2, 2, 2, 1, 2, 2],
    "newssource-10": [2, 1, 2, 2, 1, 2, 1],
    "socialmedia-1": [1, 2, 1, 1, 2, 1, 2],
    "socialmedia-2": [1, 2, 1, 1, 2, 1, 2],
    "socialmedia-3": [2, 2, 1, 2, 2, 1, 2],
    "gender": [1, 2, 2, 1, 1, 1, 2]
}

