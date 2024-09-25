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
        self._dset = None

    def process_result(self, result, parameters=None):
        if isinstance(result.payload, Start):
            logger.info("start message")
            if self._fh is None:
                name, ext = os.path.splitext(result.payload.filename)
                dest_filename = f"{name}_processed{ext}"
                os.makedirs(os.path.dirname(dest_filename), exist_ok=True)
                self._fh = h5py.File(dest_filename, 'w')
                self._dset = self._fh.create_dataset("projected", (1030,), maxshape=(None, ), dtype=np.float32)
                # self._fh.create_dataset("till_x", data=parameters["till_x"].value)
                # self._fh.create_dataset("from_y", data=parameters["from_y"].value)
        elif isinstance(result.payload, Result):
            logger.debug("got result %s", result.payload)
            oldsize = self._dset.shape[0]
            self._dset.resize(max(1 + result.event_number, oldsize), axis=0)
            self._dset[result.event_number-1] = result.payload.projections

    def finish(self, parameters=None):
        if self._fh is not None:
            self._fh.close()