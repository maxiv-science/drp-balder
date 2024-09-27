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
    projected_corr: list[int]
    roi_sum: int

class BalderWorker:
    def __init__(self, *args, **kwargs):
        self.xes_stream = "xes_eiger"
        self.coeffs = None
        self.X = None

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

    def update_correction(self, parameters, shape):
        coeffs = (parameters["a2"].value,
                 parameters["a1"].value,
                 parameters["a0"].value,
                 )
        if coeffs != self.coeffs:
            logger.debug(f"updating correction {coeffs=}")
            poly = np.poly1d(coeffs)
            x = np.arange(shape[1], dtype=np.float32)
            y = np.arange(shape[0], dtype=np.float32)
            self.X = np.meshgrid(x, y)[0]
            correction = (poly(y) - poly[0]).reshape(-1, 1)
            self.X -= correction
            self.X = self.X.reshape(-1)
            self.coeffs = coeffs
        

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
                self.update_correction(parameters, img.shape)
                mask = img < parameters["mask_greater_than"].value
                img *= mask
                projected = np.sum(img,axis=0)
                w = img.reshape(-1)
                bins = np.arange(img.shape[1]+1)
                proj_corrected, _ = np.histogram(self.X, weights=w, bins=bins)
                
                logger.debug(f"{projected=}")
                logger.debug(f"{proj_corrected=}")
                a = parameters["ROI_from"].value
                b = parameters["ROI_to"].value
                roi_sum = np.sum(proj_corrected[a:b])
                return Result(projected, proj_corrected, roi_sum)