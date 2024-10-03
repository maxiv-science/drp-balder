import logging
from copy import copy

import h5py
import h5pyd
import os
import numpy as np

from .worker import Start, Result

logger = logging.getLogger(__name__)

class BalderReducer:
    def __init__(self, *args, **kwargs):
        self.hsds = h5pyd.File("http://balder-pipeline-hsds.daq.maxiv.lu.se/home/live", username="admin",
                               password="admin", mode="a")
        self.first_run = True
        self.publish = {"map": {}, "control":{}}
        self.projections = []
        self._fh = None
        self._proj_dset = None
        self._proj_corr_dset = None
        self._roi_dset = None
        self.dir = "/entry/instrument/eiger_xes"
        self.last_roi_len = 0

    def process_result(self, result, parameters=None):
        if isinstance(result.payload, Start):
            logger.info("start message")
            if self._fh is None:
                name, ext = os.path.splitext(result.payload.filename)
                dest_filename = f"{name}_processed{ext}"
                os.makedirs(os.path.dirname(dest_filename), exist_ok=True)
                self._fh = h5py.File(dest_filename, 'w')
                self._fh.create_dataset(f"{self.dir}/ROI_limits", data=(parameters["ROI_from"].value, parameters["ROI_to"].value))
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
            self._proj_dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._proj_dset[result.event_number-1] = result.payload.projected
            self._proj_corr_dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._proj_corr_dset[result.event_number-1] = result.payload.projected_corr

            oldsize = self._roi_dset.shape[0]
            self._roi_dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._roi_dset[result.event_number-1] = result.payload.roi_sum


    def timer(self):
        logger.info("timed")
        if (self._roi_dset is not None):
            if self.last_roi_len == 0:
                self.hsds.require_group("xes")
                try: del self.hsds["xes"]["roi_sum"]
                except: pass
                try: del self.hsds["xes"]["proj_corrected"]
                except: pass
                dt_fields = self._roi_dset.dtype
                # dt_fields = np.dtype({'names': ['roi_sum'],
                #                 'formats': [(self._roi_dset.dtype)]})
                self.hsds["xes"].require_dataset("roi_sum", shape=(0,), 
                                                maxshape=(None,),
                                                dtype=dt_fields)  
                size = self._proj_corr_dset.shape[1]
                self.hsds["xes"].require_dataset("proj_corrected", shape=(0, size), 
                                                maxshape=(None, size),
                                                dtype=self._proj_corr_dset.dtype)  


            if self.last_roi_len < self._roi_dset.shape[0]:
                self.hsds["xes/roi_sum"].resize(self._roi_dset.shape[0], axis=0)
                self.hsds["xes/proj_corrected"].resize(self._roi_dset.shape[0], axis=0)
                a, b = self.last_roi_len, self._roi_dset.shape[0]
                self.hsds["xes/roi_sum"][a:b] = self._roi_dset[a:b]

                self.hsds["xes/proj_corrected"][a:b] = self._proj_corr_dset[a:b]
                self.last_roi_len = self._roi_dset.shape[0]
        return 1


    def finish(self, parameters=None):
        self.timer()
        if self._fh is not None:
            self._fh.close()
        self.hsds.close()
