import itertools
import logging

from dranspose.event import InternalWorkerMessage, StreamData
from dranspose.data.stream1 import Stream1Start, Stream1Data, Stream1End
import h5py
from bitshuffle import compress_lz4

logger = logging.getLogger(__name__)


class XESSource:  # Only works with old xes-receiver files
    def __init__(self):
        self.fname = "data/eiger-5729_data_000003.h5"
        self.fd = h5py.File(self.fname)
        self.dset = self.fd["/entry/data/data"]

    def get_source_generators(self):
        return [self.eiger_source()]

    def eiger_source(self):
        msg_number = itertools.count(0)

        stins_start = Stream1Start(
            htype="header", filename=self.fname, msg_number=next(msg_number)
        ).model_dump_json()
        start = InternalWorkerMessage(
            event_number=0,
            streams={"eigerxes": StreamData(typ="STINS", frames=[stins_start])},
        )
        logger.debug(f"Sending {start=}")
        yield start

        frameno = 0
        for image in self.dset:
            stins = Stream1Data(
                htype="image",
                msg_number=next(msg_number),
                frame=frameno,
                shape=image.shape,
                compression="bslz4",
                type=str(image.dtype),
            ).model_dump_json()
            dat = compress_lz4(image)
            img = InternalWorkerMessage(
                event_number=frameno + 1,
                streams={
                    "eigerxes": StreamData(typ="STINS", frames=[stins, dat.tobytes()])
                },
            )
            yield img
            frameno += 1
            # logger.debug(f"Sending {img=}")

        stins_end = Stream1End(
            htype="series_end", msg_number=next(msg_number)
        ).model_dump_json()
        end = InternalWorkerMessage(
            event_number=frameno,
            streams={"eigerxes": StreamData(typ="STINS", frames=[stins_end])},
        )
        logger.debug(f"Sending {end=}")
        yield end
