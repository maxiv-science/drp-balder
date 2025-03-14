import logging
import threading
from glob import glob

import h5pyd
from dranspose.replay import replay


def test_replay_h5() -> None:
    replay(
        "src.worker:BalderWorker",
        "src.reducer:BalderReducer",
        None,
        "src.hdf5_sources:XESSource",
        "params.json",
    )


def test_livequery() -> None:
    stop_event = threading.Event()
    done_event = threading.Event()

    thread = threading.Thread(
        target=replay,
        args=(
            "src.worker:BalderWorker",
            "src.reducer:BalderReducer",
            list(glob("data/*0398d78d1782.cbors")),
            None,
            "params.json",
        ),
        kwargs={"port": 5010, "stop_event": stop_event, "done_event": done_event},
    )
    thread.start()

    # do live queries

    done_event.wait()

    f = h5pyd.File("http://localhost:5010/", "r")
    logging.info("file %s", list(f.keys()))
    logging.info("xes %s", list(f["xes"].keys()))
    assert list(f.keys()) == ["xes"]
    assert list(f["xes"].keys()) == [
        "roi_sum",
        "proj_corrected",
        "last_proj_corr",
        "last_frame",
    ]
    assert f["xes/roi_sum/data"].shape == (41,)
    assert f["xes/proj_corrected/frame"].shape == (41, 1030)
    assert f["xes/roi_sum/data"][17] == 102
    stop_event.set()

    thread.join()
