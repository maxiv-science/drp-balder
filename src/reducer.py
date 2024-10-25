import logging
from copy import copy
from threading import Lock
import h5py
import h5pyd
import os
import numpy as np

from .worker import Start, Result


logger = logging.getLogger(__name__)

class BalderReducer:
    def __init__(self, *args, **kwargs):
        # self.hsds = h5pyd.File("http://balder-pipeline-hsds.daq.maxiv.lu.se/home/live", username="admin",
        #                        password="admin", mode="a")
        self.publish = {"map": {}, "control":{}}
        self.projections = []
        self._fh = None
        self._proj_dset = None
        self._proj_corr_dset = None
        self._roi_dset = None
        self.dir = "/entry/instrument/eiger_xes"
        self.last_roi_len = 0
        # self.lock = Lock()

    def process_result(self, result, parameters=None):
        if isinstance(result.payload, Start):
            logger.info("start message")
            if self._fh is None:
                name, ext = os.path.splitext(result.payload.filename)
                dest_filename = f"{name}_processed{ext}"
                try:
                    os.makedirs(os.path.dirname(dest_filename), exist_ok=True)
                    self._fh = h5py.File(dest_filename, 'w')
                except Exception:
                    self._fh = h5py.File(dest_filename, 'w', driver='core', backing_store=False)
                    logger.warning(f"Could not write to file {dest_filename}. Will work in live mode only.")
                limits = (parameters["ROI_from"].value, parameters["ROI_to"].value)
                self._fh.create_dataset(f"{self.dir}/ROI_limits", data=limits)
                coeffs = (parameters["a0"].value, parameters["a1"].value, parameters["a2"].value)
                self._fh.create_dataset(f"{self.dir}/coefficients", data=coeffs)
        elif isinstance(result.payload, Result):
            logger.debug("got result %s", result.payload)
            if self._proj_dset is None:
                size = result.payload.projected.shape[0]
                dtype = result.payload.projected.dtype
                self._proj_dset = self._fh.create_dataset(f"{self.dir}/proj", (0, size), maxshape=(None, size), dtype=dtype)
                dtype = result.payload.projected_corr.dtype
                self._proj_corr_dset = self._fh.create_dataset(f"{self.dir}/proj_corrected", (0, size), maxshape=(None, size), dtype=dtype)
                self._roi_dset = self._fh.create_dataset(f"{self.dir}/ROI_sum", (0, ), maxshape=(None, ), dtype=dtype)
            oldsize = self._proj_dset.shape[0]
            newsize = max(1 + result.event_number, oldsize)
            self._proj_dset.resize(newsize, axis=0)
            self._proj_dset[result.event_number-1] = result.payload.projected
            self._proj_corr_dset.resize(newsize, axis=0)
            self._proj_corr_dset[result.event_number-1] = result.payload.projected_corr
            self._roi_dset.resize(newsize, axis=0)
            self._roi_dset[result.event_number-1] = result.payload.roi_sum
            if result.payload.preview is not None:
                self.publish["last_frame"] = result.payload.preview
                self.publish["last_proj_corr"] = result.payload.projected_corr
                self.publish["last_proj"] = result.payload.projected
                self.publish["roi_sum"] = np.array(self._roi_dset)
                self.publish["proj_corrected"] = np.array(self._proj_corr_dset)
                self.publish["proj_corrected"] = np.array(self._proj_corr_dset)
            self.last_roi_len = min(self.last_roi_len, result.event_number-1)


    # def timer(self):
    #     logger.info("timer called")
    #     next_timer = 0.5
    #     if (self._fh is None) or (self.hsds is None):
    #         return next_timer
    #     with self.lock:
    #         if (self._roi_dset is not None):
    #             if self.last_roi_len == 0:
    #                 self.hsds.require_group("xes")
    #                 for dname in ("roi_sum", "proj_corrected", "projected", "last_frame", "last_proj","last_proj_corr"): 
    #                     try: 
    #                         del self.hsds["xes"][dname]
    #                     except Exception: 
    #                         pass
    #                 dt_fields = self._roi_dset.dtype
    #                 # dt_fields = np.dtype({'names': ['roi_sum'],
    #                 #                 'formats': [(self._roi_dset.dtype)]})
    #                 self.hsds["xes"].require_dataset("roi_sum", shape=(0,), 
    #                                                 maxshape=(None,),
    #                                                 dtype=dt_fields)  
    #                 size = self._proj_corr_dset.shape[1]
    #                 self.hsds["xes"].require_dataset("proj_corrected", 
    #                                                 shape=(0, size), 
    #                                                 maxshape=(None, size),
    #                                                 dtype=self._proj_corr_dset.dtype)  
    #                 self.hsds["xes"].require_dataset("projected", 
    #                                                 shape=(0, size), 
    #                                                 maxshape=(None, size),
    #                                                 dtype=self._proj_dset.dtype)  

    #             if self.last_roi_len < self._roi_dset.shape[0]:
    #                 self.hsds["xes/roi_sum"].resize(self._roi_dset.shape[0], axis=0)
    #                 self.hsds["xes/proj_corrected"].resize(self._roi_dset.shape[0], axis=0)
    #                 self.hsds["xes/projected"].resize(self._roi_dset.shape[0], axis=0)
    #                 a, b = 0, self._roi_dset.shape[0]
    #                 self.hsds["xes/roi_sum"][a:b] = self._roi_dset[a:b]
    #                 self.hsds["xes/proj_corrected"][a:b] = self._proj_corr_dset[a:b]
    #                 self.hsds["xes/projected"][a:b] = self._proj_dset[a:b]
    #                 self.last_roi_len = b
    #             if "last_frame" in self.publish:
    #                 logger.info("create live frame preview in hsds")
    #                 if "last_frame" not in self.hsds["xes"]: 
    #                     self.hsds["xes"].require_dataset("last_frame", 
    #                                                     shape=self.publish["last_frame"].shape, 
    #                                                     maxshape=self.publish["last_frame"].shape,
    #                                                     dtype=self.publish["last_frame"].dtype) 
    #                     self.hsds["xes"].require_dataset("last_proj", 
    #                                                     shape=self.publish["last_proj"].shape, 
    #                                                     maxshape=self.publish["last_proj"].shape,
    #                                                     dtype=self.publish["last_proj"].dtype) 
    #                     self.hsds["xes"].require_dataset("last_proj_corr", 
    #                                                     shape=self.publish["last_proj"].shape, 
    #                                                     maxshape=self.publish["last_proj"].shape,
    #                                                     dtype=self.publish["last_proj"].dtype) 
    #                 self.hsds["xes/last_frame"][:] = self.publish["last_frame"]
    #                 self.hsds["xes/last_proj"][:] = self.publish["last_proj"]
    #                 self.hsds["xes/last_proj_corr"][:] = self.publish["last_proj_corr"]

    #         return next_timer


    def finish(self, parameters=None):
        # self.timer()
        logger.info("FINISH THEM!!!")
        # with self.lock:
        #     if self._fh is not None:
        #         self._fh.close()
        #         self._fh = None
        #     try:
        #         self.hsds.close()
        #     except Exception:
        #         pass
        #     self.hsds = None
        if self._fh is not None:
            self._fh.close()
            self._fh = None
