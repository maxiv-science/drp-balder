import logging
from dataclasses import dataclass
import numpy as np

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End
from dranspose.parameters import IntParameter

from bitshuffle import decompress_lz4
import zmq

logger = logging.getLogger(__name__)

@dataclass
class Start:
    filename: str

@dataclass
class Result:
    projected: list[float]

class XESWorker:
    def __init__(self, *args, **kwargs):
        self.xes_stream = "xes_eiger"

    # @staticmethod
    # def describe_parameters():
    #     params = [
    #         IntParameter(name="till_x", default=2),
    #         IntParameter(name="from_y", default=8),
    #     ]
    #     return params

    def process_event(self, event, parameters=None):
        # logger.debug(event)

        if self.xes_stream in event.streams:
            logger.debug(f"{self.xes_stream} found")
            acq = parse_stins(event.streams[self.xes_stream])
            logger.debug(f"message parsed {acq}")
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return Start(acq.filename)
            elif isinstance(acq, Stream1Data):
                if 'bslz4' in acq.compression:
                    bufframe = event.streams[self.xes_stream].frames[1]
                    if isinstance(bufframe, zmq.Frame):
                        bufframe = bufframe.bytes
                    img = decompress_lz4(bufframe, acq.shape, dtype=acq.type)
                    projection = np.sum(img,axis=1)
                    logger.debug(f"{projection=}")
                    return Result(projection)