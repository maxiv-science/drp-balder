import logging
import pickle
from dataclasses import dataclass
from typing import Optional
import numpy as np
from math import sin, radians

import json
from dranspose.middlewares.stream1 import parse as parse_stins
from dranspose.middlewares.positioncap import PositioncapParser
from dranspose.middlewares.sardana import parse as parse_sardana
from dranspose.data.stream1 import Stream1Data, Stream1Start, Stream1End
from dranspose.data.positioncap import PositionCapStart, PositionCapValues
from dranspose.data.sardana import SardanaDataDescription, SardanaRecordData
from dranspose.parameters import (
    IntParameter,
    FloatParameter,
    BinaryParameter,
    ParameterName,
)

# from dranspose.middlewares.sardana import parse as sardana_parse
from bitshuffle import decompress_lz4
import zmq

logger = logging.getLogger(__name__)


@dataclass
class Start:
    filename: str
    motor_name: str


@dataclass
class Result:
    projected: list[int]
    projected_corr: list[int]
    roi_sum: int
    motor_pos: float
    preview: Optional[list[int, int]] = None


class BalderWorker:
    def __init__(self, *args, **kwargs):
        self.xes_stream = "eigerxes"
        self.sardana_stream = "sardana"
        self.pcap_stream = "pcap"
        self.pcap = PositioncapParser()
        self.coeffs = None
        self.pcap = PositioncapParser()
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
            BinaryParameter(name=ParameterName("mask"), default=b""),
        ]
        return params

    def get_tick_interval(self, parameters=None):
        return 0.5

    def _update_correction(self, parameters, shape):
        new_coeffs = (
            parameters["a2"].value,
            parameters["a1"].value,
            parameters["a0"].value,
        )
        if new_coeffs != self.coeffs:
            logger.debug(f"updating correction {new_coeffs=}")
            poly = np.poly1d(new_coeffs)
            x = np.arange(shape[1], dtype=np.float32)
            y = np.arange(shape[0], dtype=np.float32)
            self.X = np.meshgrid(x, y)[0]
            correction = (poly(y) - poly[0]).reshape(-1, 1)
            self.X -= correction
            self.X = self.X.reshape(-1)
            self.coeffs = new_coeffs

    def process_event(self, event, parameters=None, tick=False, *args, **kwargs):
        # if self.pcap_stream in event.streams:
        #     res = self.pcap.parse(event.streams[self.pcap_stream])
        #     if isinstance(res, PositionCapStart):
        #         self.arm_time = res.arm_time
        #     elif isinstance(res, PositionCapValues):
        #         triggernumber = res.fields["COUNTER2.OUT.Max"].value
        #         ene_raw = res.fields["INENC2.VAL.Mean"].value
        #         crystalconstant = 3.1346797943115234 # FIXME this should be read from the tango device
        #         ene = 12398.419/(2*crystalconstant*sin(radians(ene_raw/874666)))
        #         logger.debug(f"{triggernumber=} {ene_raw=} {ene=}")
        if self.sardana_stream in event.streams:
            res = parse_sardana(event.streams[self.sardana_stream])
            logger.debug(f"{res=} found")
            if isinstance(res, SardanaDataDescription):
                self.motor = res.ref_moveables[0]
                # self.motor = res.title.split(" ")[1]
                logger.debug(f"{self.motor=}")
            elif isinstance(res, SardanaRecordData):
                motor_pos = getattr(res, self.motor)
                logger.debug(f"{motor_pos=}")
        else:
            motor_pos = 0

        if self.xes_stream in event.streams:
            logger.debug(f"{self.xes_stream} found")
            acq = parse_stins(event.streams[self.xes_stream])
            logger.debug(f"message parsed {acq}")
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return Start(acq.filename, self.motor)
            elif isinstance(acq, Stream1Data):
                logger.info("image message %s", acq)
                if "bslz4" in acq.compression:
                    bufframe = event.streams[self.xes_stream].frames[1]
                    if isinstance(bufframe, zmq.Frame):
                        bufframe = bufframe.bytes
                    img = decompress_lz4(bufframe, acq.shape, dtype=acq.type)
                else:
                    img = acq.data
                self._update_correction(parameters, img.shape)
                if parameters["mask"].value != b"":
                    logger.debug("Mask found in parameters")
                    mask = pickle.loads(parameters["mask"].value)
                    logger.debug(f"{mask=}")
                else:
                    logger.debug(
                        f"No mask found, masking values above {parameters['mask_greater_than']}"
                    )
                    mask = img >= parameters["mask_greater_than"].value
                masked_img = img * ~mask
                projected = np.sum(masked_img, axis=0)
                w = masked_img.reshape(-1)
                bins = np.arange(masked_img.shape[1] + 1)
                proj_corrected, _ = np.histogram(self.X, weights=w, bins=bins)

                a = parameters["ROI_from"].value
                b = parameters["ROI_to"].value
                roi_sum = np.sum(proj_corrected[a:b])
                if tick:
                    logger.info("Tick received, sending live preview...")
                    return Result(
                        projected, proj_corrected, roi_sum, motor_pos, masked_img[:]
                    )
                else:
                    return Result(projected, proj_corrected, roi_sum, motor_pos)
