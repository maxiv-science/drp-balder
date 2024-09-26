import logging
from dataclasses import dataclass
import numpy as np

from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End
from dranspose.parameters import IntParameter, FloatParameter

from bitshuffle import decompress_lz4
import zmq

logger = logging.getLogger(__name__)

@dataclass
class Start:
    filename: str

@dataclass
class Result:
    projected: list[int]
    roi_sum: int

class XESWorker:
    def __init__(self, *args, **kwargs):
        self.xes_stream = "xes_eiger"

    @staticmethod
    def describe_parameters():
        params = [
            IntParameter(name="ROI_from", default=0),
            IntParameter(name="ROI_to", default=1065),
            IntParameter(name="mask_greater_than", default=0xFFFFFFFF - 1),
            FloatParameter(name="a0", default=0),
            FloatParameter(name="a1", default=0),
            FloatParameter(name="a2", default=0),
        ]
        return params

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
                else:
                    img = acq.data
                masked_img = np.ma.masked_greater(img, parameters["mask_greater_than"].value)
                projected = np.sum(masked_img,axis=0)
                logger.debug(f"{projected=}")
                a = parameters["ROI_from"].value
                b = parameters["ROI_to"].value
                roi_sum = np.sum(projected[a:b])
                return Result(projected, roi_sum)