import logging
from dataclasses import dataclass
import numpy as np

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End
from dranspose.parameters import IntParameter

logger = logging.getLogger(__name__)

@dataclass
class Start:
    filename: str

@dataclass
class Result:
    projected: list[float]

class XESWorker:
    def __init__(self, *args, **kwargs):
        self.stream_name = "xes_eiger"

    # @staticmethod
    # def describe_parameters():
    #     params = [
    #         IntParameter(name="till_x", default=2),
    #         IntParameter(name="from_y", default=8),
    #     ]
    #     return params

    def process_event(self, event, parameters=None):
        # logger.debug(event)
        if self.stream_name in event.streams:
            logger.debug(f"{self.stream_name} found")
            acq = parse_stins(event.streams[self.stream_name])
            logger.debug(f"message parsed {acq}")
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return Start(acq.filename)
            elif isinstance(acq, Stream1Data):
                projection = np.sum(acq.data,axis=1)
                logger.debug(f"{projection=}")
                return Result(projection)