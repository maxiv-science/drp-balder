from dranspose.event import ResultData


class BalderReducer:
    def __init__(self, parameters=None):
        self.publish = {"map": {}, "control":{}}

    def process_result(self, result: ResultData, parameters=None):
        pass

    def finish(self, parameters=None):
        print("finished reducer")
