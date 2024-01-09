import json
import logging

from dranspose.event import EventData
from dranspose.parameters import StrParameter, FileParameter
import numpy as np

logger = logging.getLogger(__name__)

class BalderWorker:

    def __init__(self, parameters=None):
        self.number = 0

    def process_event(self, event: EventData, parameters=None):
        logger.debug("using parameters %s", parameters)


    def finish(self, parameters=None):
        print("finished")
