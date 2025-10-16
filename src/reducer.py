import logging
import time
import json

from typing import Any

import h5py
import os
import numpy as np

from dranspose.event import ResultData
from dranspose.protocol import ParameterName, WorkParameter
from dranspose.parameters import StrParameter, ParameterBase
from readerwriterlock.rwlock import RWLockFair

from .worker import Start, Result


logger = logging.getLogger(__name__)


class BalderReducer:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.rw_lock = RWLockFair()
        self.publish_rlock = self.rw_lock.gen_rlock()
        self.publish_wlock = self.rw_lock.gen_wlock()

        self.last_event = -1
        self.last_processed = -1
        self.new_band_roi = False
        self.band_roi = {}
        # FIXME make last_frame nxData and fix proj_corr in slix view
        self.roi_sum: dict[str, Any] = {
            "data_attrs": {"long_name": "photons"},
            "data": None,
            "motor_attrs": {"long_name": "movable"},
            "motor": None,
        }
        self.proj_corrected: dict[str, Any] = {
            "motor_attrs": {"long_name": "movable"},
            "frame": None,
            "motor": None,
        }
        self.band_left: dict[str, Any] = {
            "data_attrs": {"long_name": "photons"},
            "data": None,
            "motor_attrs": {"long_name": "movable"},
            "motor": None,
        }
        self.band_bottom: dict[str, Any] = {
            "data_attrs": {"long_name": "photons"},
            "data": None,
            "motor_attrs": {"long_name": "movable"},
            "motor": None,
        }
        self.pub_xes: dict[str, Any] = {
            "roi_sum": self.roi_sum,
            "roi_sum_attrs": {
                "NX_class": "NXdata",
                "axes": ["motor"],
                "signal": "data",
            },
            "proj_corrected": self.proj_corrected,
            "proj_corrected_attrs": {
                "NX_class": "NXdata",
                "axes": [".", "motor"],
                "signal": "frame",
                "interpretation": "image",
            },
            "band_theta": self.band_left,
            "band_theta_attrs": {
                "NX_class": "NXdata",
                "axes": ["motor"],
                "signal": "data",
            },
            "band_2theta": self.band_bottom,
            "band_2theta_attrs": {
                "NX_class": "NXdata",
                "axes": ["motor"],
                "signal": "data",
            },
        }
        self.publish = {"xes": self.pub_xes}
        # self.projections: list[Any] = []
        self._fh: h5py.File | None = None
        self._proj_dset: h5py.Dataset | None = None
        self._proj_corr_dset: h5py.Dataset | None = None
        # self._roi_dset: h5py.Dataset | None = None
        self._pos_dset: h5py.Dataset | None = None
        self.dir = "/entry/instrument/eiger_xes_processed"
        self.limits = (0, 0)

    @staticmethod
    def describe_parameters() -> list[ParameterBase]:
        params = [
            StrParameter(name=ParameterName("BandROI"), default="{}"),
        ]
        return params

    def process_result(
        self, result: ResultData, parameters: dict[ParameterName, WorkParameter]
    ) -> None:
        try:
            band_roi = json.loads(parameters[ParameterName("BandROI")].value)
            if band_roi != self.band_roi:
                self.band_roi = band_roi
                self.new_band_roi = True
        except KeyError:
            logger.warning("Could not decode BandROI")
        self.limits = (
            parameters[ParameterName("ROI_from")].value,
            parameters[ParameterName("ROI_to")].value,
        )
        if isinstance(result.payload, Start):
            logger.info("start message")
            self.roi_sum["motor_attrs"]["long_name"] = result.payload.motor_name
            self.proj_corrected["motor_attrs"]["long_name"] = result.payload.motor_name
            self.band_left["motor_attrs"]["long_name"] = result.payload.motor_name
            self.band_bottom["motor_attrs"]["long_name"] = result.payload.motor_name
            if self._fh is None:
                name, ext = os.path.splitext(result.payload.filename)
                dest_filename = f"{name}_processed{ext}"
                try:
                    os.makedirs(os.path.dirname(dest_filename), exist_ok=True)
                    self._fh = h5py.File(dest_filename, "w")
                except Exception:
                    self._fh = h5py.File(
                        dest_filename, "w", driver="core", backing_store=False
                    )
                    logger.warning(
                        f"Could not write to file {dest_filename}. Will work in live mode only."
                    )

                self._fh.create_dataset(f"{self.dir}/ROI_limits", data=self.limits)
                coeffs = (
                    parameters[ParameterName("a0")].value,
                    parameters[ParameterName("a1")].value,
                    parameters[ParameterName("a2")].value,
                )
                self._fh.create_dataset(f"{self.dir}/coefficients", data=coeffs)
                self._fh.create_dataset(
                    f"{self.dir}/band_roi",
                    data=parameters[ParameterName("BandROI")].value,
                )

        elif isinstance(result.payload, Result):
            self.last_event = result.event_number
            logger.debug("got result %s", result.payload)
            if self._proj_dset is None and self._fh is not None:
                size = result.payload.projected.shape[0]
                dtype = result.payload.projected.dtype
                self._proj_dset = self._fh.create_dataset(
                    f"{self.dir}/proj", (0, size), maxshape=(None, size), dtype=dtype
                )
                dtype = result.payload.projected_corr.dtype
                self._proj_corr_dset = self._fh.create_dataset(
                    f"{self.dir}/proj_corrected",
                    (0, size),
                    maxshape=(None, size),
                    dtype=dtype,
                )
                self._fh[f"{self.dir}/data"] = h5py.SoftLink(
                    f"{self.dir}/proj_corrected"
                )
                # self._roi_dset = self._fh.create_dataset(
                #     f"{self.dir}/ROI_sum", (0,), maxshape=(None,), dtype=dtype
                # )
                self._pos_dset = self._fh.create_dataset(
                    f"{self.dir}/motor_pos", (0,), maxshape=(None,), dtype="float"
                )
            assert self._proj_dset is not None
            assert self._proj_corr_dset is not None
            # assert self._roi_dset is not None
            assert self._pos_dset is not None
            oldsize = self._proj_dset.shape[0]
            newsize = max(result.event_number, oldsize)
            self._proj_dset.resize(newsize, axis=0)
            self._proj_dset[result.event_number - 1] = result.payload.projected
            self._proj_corr_dset.resize(newsize, axis=0)
            self._proj_corr_dset[
                result.event_number - 1
            ] = result.payload.projected_corr
            # self._roi_dset.resize(newsize, axis=0)
            # self._roi_dset[result.event_number - 1] =
            self._pos_dset.resize(newsize, axis=0)
            self._pos_dset[result.event_number - 1] = result.payload.motor_pos
            with self.publish_wlock:
                # publish results and live preview
                if result.payload.preview is not None:
                    self.pub_xes["last_frame"] = result.payload.preview
                self.proj_corrected["frame"] = np.array(self._proj_corr_dset)
                self.proj_corrected["motor"] = np.array(self._pos_dset)
                self.pub_xes["last_proj_corr"] = result.payload.projected_corr

    def timer(self):
        start = time.time()
        logger.debug(f"{self.band_roi=}")
        if (not self.new_band_roi) and self.last_event == self.last_processed:
            logger.debug("No new ROI or new events, nothing to process")
            return 1
        self.new_band_roi = False
        self.last_processed = self.last_event
        try:
            x1, y1 = self.band_roi["begin"]
            x2, y2 = self.band_roi["end"]
            w = self.band_roi["width"]
            k = (y2 - y1) / (x2 - x1)
            b = y2 - k * x2
            useFractionalPixels = self.band_roi.get("useFractionalPixels", True)
        except Exception as e:
            logger.warning(
                "Timer: Could not get band ROI info %s, %s", str(self.band_roi), e
            )
            return 1
        if (self.proj_corrected["frame"] is None) or (
            self.proj_corrected["motor"] is None
        ):
            logger.warning("Timer: No image to analyse")
            return 1
        with self.publish_rlock:
            yRange = self.proj_corrected["motor"][0], self.proj_corrected["motor"][-1]
            dataCut = np.array(self.proj_corrected["frame"], dtype=np.float32)
            ny, nx = self.proj_corrected["frame"].shape

        data_x = np.arange(nx)
        data_y = np.linspace(*yRange, ny)

        u, v = np.meshgrid(data_x, data_y)
        if len(data_y) > 1:
            dt = abs(data_y[-1] - data_y[0]) / (len(data_y) - 1)
        else:
            dt = 1
        vm = v - k * u - b - w / 2
        vp = v - k * u - b + w / 2
        if useFractionalPixels and (dt > 0):
            dataCut[vm > dt] = 0
            dataCut[vp < -dt] = 0
            vmWherePartial = (vm > 0) & (vm < dt)
            dataCut[vmWherePartial] *= vm[vmWherePartial] / dt
            vpWherePartial = (vp > -dt) & (vp < 0)
            dataCut[vpWherePartial] *= -vp[vpWherePartial] / dt
        else:
            dataCut[vm > 0] = 0
            dataCut[vp < 0] = 0
        proj_left = dataCut.sum(axis=1)

        proj_bottom = dataCut.sum(axis=0)
        x_bottom = k * data_x + b

        # cutting of the incomplete ends:
        gd = (x_bottom - w / 2 > data_y[0]) & (x_bottom + w / 2 < data_y[-1])
        proj_bottom = proj_bottom[gd]

        with self.publish_wlock:
            logger.info("Timer: Publish projections")
            self.band_left["data"] = proj_left
            self.band_left["motor"] = data_y
            self.band_bottom["data"] = proj_bottom
            self.band_bottom["motor"] = x_bottom
            a, b = self.limits
            self.roi_sum["data"] = np.sum(self.proj_corrected["frame"][a:b], axis=0)
            self.roi_sum["motor"] = self.proj_corrected["motor"]
            # self.ds_dt = np.dtype(
            #     {"names": col_names, "formats": [(float)] * len(col_names)}
            # )
            # self.publish["pcap"] = np.array([], dtype=self.ds_dt)

        end = time.time()
        return max(0, 1 - (end - start))
        #  try to run at 1Hz

    def finish(
        self, parameters: dict[ParameterName, WorkParameter] | None = None
    ) -> None:
        logger.info("FINISH THEM!!!")
        if self._fh is not None:
            # add datasets for the latest ROI sums
            self._fh.create_dataset(f"{self.dir}/ROI_sum", data=self.roi_sum["data"])
            self._fh.create_dataset(
                f"{self.dir}/band_theta", data=self.band_left["data"]
            )
            self._fh.create_dataset(
                f"{self.dir}/band_2theta", data=self.band_bottom["data"]
            )
            self._fh.close()
