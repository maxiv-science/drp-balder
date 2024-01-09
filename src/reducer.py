from dranspose.event import ResultData


class FluorescenceReducer:
    def __init__(self, parameters=None):
        self.publish = {"map": {}, "control":{}}

    def process_result(self, result: ResultData, parameters=None):

    def finish(self, parameters=None):
        print("finished reducer")
