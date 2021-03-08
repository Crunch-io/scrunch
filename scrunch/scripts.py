# coding: utf-8


class DatasetScripts:
    def __init__(self, dataset):
        self.dataset = dataset
        self.dataset_resource = dataset.resource

    def execute(self, script_body):
        self.dataset_resource.scripts.create({
            'element': 'shoji:entity',
            'body': {"body": script_body},
        })

    def collapse(self):
        print("COLLAPSE")
        self.dataset_resource.scripts.collapse.post({})
        print("COLLAPSED")

    def all(self):
        scripts_index = self.dataset_resource.scripts.index
        scripts = []
        for s_url, s in scripts_index.items():
            scripts.append(s.entity)
        scripts = sorted(scripts, key=lambda s: s.body["creation_time"])
        return scripts

    def revert(self, id=None, script_number=None):
        assert False
