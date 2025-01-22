import logging
from copy import copy
from threading import Lock
from typing import Any

import h5py
import h5pyd
import os
import numpy as np

from dranspose.event import ResultData
from dranspose.protocol import ParameterName, WorkParameter

from .worker import Start, Result


logger = logging.getLogger(__name__)


class BalderReducer:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # self.hsds = h5pyd.File("http://balder-pipeline-hsds.daq.maxiv.lu.se/home/live", username="admin",
        #                        password="admin", mode="a")
        self.roi_sum: dict[str, Any] = {
            "data_attrs": {"long_name": "photons"},
            # "data": np.ones((42)),
            "motor_attrs": {"long_name": "motor"},
            # "motor": np.linspace(0,1,42),
        }
        self.proj_corrected: dict[str, Any] = {
            "motor_attrs": {"long_name": "motor"},
            #    "frame": np.ones((42, 42)),
            #    "motor": np.linspace(0, 1, 42),
        }
        self.pub_xes: dict[str, Any] = {
            "roi_sum_attrs": {
                "NX_class": "NXdata",
                "axes": ["motor"],
                "signal": "data",
            },
            "roi_sum": self.roi_sum,
            "proj_corrected_attrs": {
                "NX_class": "NXdata",
                "axes": ["motor", "."],
                "signal": "frame",
                "interpretation": "image",
            },
            "proj_corrected": self.proj_corrected,
        }
        self.publish = {"xes": self.pub_xes}
        # self.projections: list[Any] = []
        self._fh: h5py.File | None = None
        self._proj_dset: h5py.Dataset | None = None
        self._proj_corr_dset: h5py.Dataset | None = None
        self._roi_dset: h5py.Dataset | None = None
        self._pos_dset: h5py.Dataset | None = None
        self.dir = "/entry/instrument/eiger_xes_processed"
        self.last_roi_len = 0

    def process_result(
        self, result: ResultData, parameters: dict[ParameterName, WorkParameter]
    ) -> None:
        if isinstance(result.payload, Start):
            logger.info("start message")
            self.roi_sum["motor_attrs"] = {"long_name": result.payload.motor_name}
            self.roi_sum["motor_attrs"] = {"long_name": result.payload.motor_name}
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
                # self._fh["/entry/instrument/eiger_xes"] = h5py.ExternalLink(
                #     result.payload.filename.replace("eiger_xes","sardana"), "/entry/instrument/eiger_xes"
                # )
                limits = (
                    parameters[ParameterName("ROI_from")].value,
                    parameters[ParameterName("ROI_to")].value,
                )
                self._fh.create_dataset(f"{self.dir}/ROI_limits", data=limits)
                coeffs = (
                    parameters[ParameterName("a0")].value,
                    parameters[ParameterName("a1")].value,
                    parameters[ParameterName("a2")].value,
                )
                self._fh.create_dataset(f"{self.dir}/coefficients", data=coeffs)

        elif isinstance(result.payload, Result):
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
                self._roi_dset = self._fh.create_dataset(
                    f"{self.dir}/ROI_sum", (0,), maxshape=(None,), dtype=dtype
                )
                self._pos_dset = self._fh.create_dataset(
                    f"{self.dir}/motor_pos", (0,), maxshape=(None,), dtype="float"
                )
            assert self._proj_dset is not None
            assert self._proj_corr_dset is not None
            assert self._roi_dset is not None
            assert self._pos_dset is not None
            oldsize = self._proj_dset.shape[0]
            newsize = max(result.event_number, oldsize)
            self._proj_dset.resize(newsize, axis=0)
            self._proj_dset[result.event_number - 1] = result.payload.projected
            self._proj_corr_dset.resize(newsize, axis=0)
            self._proj_corr_dset[
                result.event_number - 1
            ] = result.payload.projected_corr
            self._roi_dset.resize(newsize, axis=0)
            self._roi_dset[result.event_number - 1] = result.payload.roi_sum
            self._pos_dset.resize(newsize, axis=0)
            self._pos_dset[result.event_number - 1] = result.payload.motor_pos
            # publish results and live preview
            if result.payload.preview is not None:
                self.pub_xes["last_frame"] = result.payload.preview
                self.roi_sum["data"] = np.array(self._roi_dset)
                self.roi_sum["motor"] = np.array(self._pos_dset)
                self.proj_corrected["frame"] = np.array(self._proj_corr_dset)
                self.proj_corrected["motor"] = np.array(self._pos_dset)
                self.pub_xes["last_proj_corr"] = result.payload.projected_corr

            # self.last_roi_len = min(self.last_roi_len, result.event_number-1)

    def finish(
        self, parameters: dict[ParameterName, WorkParameter] | None = None
    ) -> None:
        logger.info("FINISH THEM!!!")
        if self._fh is not None:
            self._fh.close()
