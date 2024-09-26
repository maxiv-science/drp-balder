import logging
import h5py
import os
import numpy as np

from .worker import Start, Result

logger = logging.getLogger(__name__)

class XESReducer:
    def __init__(self, *args, **kwargs):
        self.projections = []
        self._fh = None
        self._proj_dset = None
        self._roi_dset = None

    def process_result(self, result, parameters=None):
        if isinstance(result.payload, Start):
            logger.info("start message")
            if self._fh is None:
                name, ext = os.path.splitext(result.payload.filename)
                dest_filename = f"{name}_processed{ext}"
                os.makedirs(os.path.dirname(dest_filename), exist_ok=True)
                self._fh = h5py.File(dest_filename, 'w')
                self._fh.create_dataset("ROI_from", data=parameters["ROI_from"].value)
                self._fh.create_dataset("ROI_to", data=parameters["ROI_to"].value)
                self._roi_dset = self._fh.create_dataset("ROI_sum", (0, ), maxshape=(None, ), dtype=np.int32)
        elif isinstance(result.payload, Result):
            logger.debug("got result %s", result.payload)
            if self._proj_dset is None:
                size = result.payload.projected.shape[0]
                dtype = result.payload.projected.dtype
                self._proj_dset = self._fh.create_dataset("projected", (0, size), maxshape=(None, size), dtype=dtype)
            oldsize = self._proj_dset.shape[0]
            self._proj_dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._proj_dset[result.event_number-1] = result.payload.projected

            oldsize = self._roi_dset.shape[0]
            self._roi_dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._roi_dset[result.event_number-1] = result.payload.roi_sum


    def finish(self, parameters=None):
        if self._fh is not None:
            self._fh.close()