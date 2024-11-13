import logging
import os
import pickle
from dataclasses import dataclass
from typing import Optional, Any
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
    ParameterBase,
)
from dranspose.protocol import ParameterName, WorkParameter, StreamName
from dranspose.event import EventData

# from dranspose.middlewares.sardana import parse as sardana_parse
from bitshuffle import decompress_lz4
import zmq

logger = logging.getLogger(__name__)


@dataclass
class Start:
    filename: str
    sardana_filename: str
    motor_name: str


@dataclass
class Result:
    projected: np.ndarray[Any, np.dtype[Any]]
    projected_corr: np.ndarray[Any, np.dtype[Any]]
    roi_sum: int
    motor_pos: float
    preview: Optional[list[int]] = None


class BalderWorker:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.xes_stream = StreamName("eigerxes")
        self.sardana_stream = StreamName("sardana")
        self.pcap_stream = StreamName("pcap")
        self.coeffs: tuple[float, ...] | None = None
        self.pcap = PositioncapParser()
        self.X: np.ndarray[Any, np.dtype[Any]] | None = None
        self.motor = "unknown"

    @staticmethod
    def describe_parameters() -> list[ParameterBase]:
        params = [
            IntParameter(name=ParameterName("ROI_from"), default=0),
            IntParameter(name=ParameterName("ROI_to"), default=1065),
            IntParameter(
                name=ParameterName("mask_greater_than"), default=0xFFFFFFFF - 1
            ),
            FloatParameter(name=ParameterName("a0"), default=0),
            FloatParameter(name=ParameterName("a1"), default=0),
            FloatParameter(name=ParameterName("a2"), default=0),
            BinaryParameter(name=ParameterName("mask"), default=b""),
        ]
        return params

    def get_tick_interval(
        self, parameters: dict[ParameterName, WorkParameter] | None = None
    ) -> float:
        return 0.5

    def _update_correction(
        self, parameters: dict[ParameterName, WorkParameter], shape: tuple[int, int]
    ) -> None:
        new_coeffs: tuple[float, ...] = (  # type: ignore[assignment]
            parameters[ParameterName("a2")].value,
            parameters[ParameterName("a1")].value,
            parameters[ParameterName("a0")].value,
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

    def process_event(
        self,
        event: EventData,
        parameters: dict[ParameterName, WorkParameter],
        tick: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Start | Result | None:
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
        sardana_filename = ""
        motor_pos = 0
        if self.sardana_stream in event.streams:
            res = parse_sardana(event.streams[self.sardana_stream])
            logger.debug(f"{res=} found")
            if isinstance(res, SardanaDataDescription):
                logger.info(f"{res=}")
                try:
                    fnames = [f for f in res.scanfile if "h5" in f]
                    sardana_filename = os.path.join(res.scandir, fnames[0])
                    logger.debug(f"{sardana_filename=}")
                except Exception as e:
                    logger.warning(f"Could not determine Sardana scanfile: {e}")
                if hasattr(res, "ref_moveables"):
                    if len(res.ref_moveables) > 0:
                        self.motor = res.ref_moveables[0]
                        logger.debug(f"{self.motor=}")
            elif isinstance(res, SardanaRecordData):
                motor_pos = getattr(res, self.motor)
                logger.debug(f"{motor_pos=}")

        if self.xes_stream in event.streams:
            logger.debug(f"{self.xes_stream} found")
            acq = parse_stins(event.streams[self.xes_stream])
            logger.debug(f"message parsed {acq}")
            if isinstance(acq, Stream1Start):
                logger.info("start message %s", acq)
                return Start(acq.filename, sardana_filename, self.motor)

            elif isinstance(acq, Stream1Data):
                logger.info("image message %s", acq)
                if "bslz4" in acq.compression:
                    bufframe = event.streams[self.xes_stream].frames[1]
                    if isinstance(bufframe, zmq.Frame):
                        bufframe = bufframe.bytes
                    img = decompress_lz4(bufframe, acq.shape, dtype=acq.type)
                else:
                    assert hasattr(acq, "data")
                    img = acq.data
                self._update_correction(parameters, img.shape)
                mask = None
                if parameters[ParameterName("mask")].value != b"":
                    logger.debug("Mask found in parameters")
                    bin_mask = parameters[ParameterName("mask")].value
                    if isinstance(bin_mask, bytes):
                        mask = pickle.loads(bin_mask)
                        logger.debug(f"{mask=}")

                if mask is None:
                    logger.debug(
                        f"No mask found, masking values above {parameters[ParameterName('mask_greater_than')]}"
                    )
                    mask = img >= parameters[ParameterName("mask_greater_than")].value
                masked_img = img * ~mask
                projected = np.sum(masked_img, axis=0)
                w = masked_img.reshape(-1)
                bins = np.arange(masked_img.shape[1] + 1)
                if self.X is not None:
                    proj_corrected, _ = np.histogram(self.X, weights=w, bins=bins)

                    a: int = parameters[ParameterName("ROI_from")].value  # type: ignore[assignment]
                    b: int = parameters[ParameterName("ROI_to")].value  # type: ignore[assignment]
                    roi_sum = np.sum(proj_corrected[a:b])
                    if tick:
                        logger.info("Tick received, sending live preview...")
                        return Result(
                            projected, proj_corrected, roi_sum, motor_pos, masked_img[:]
                        )
                    else:
                        return Result(projected, proj_corrected, roi_sum, motor_pos)
        return None
