## Development

The worker and reducer may be developed locally by replaying ingester recorded streams.

Provide the classes for the worker and reducer as well as the ingester files.
Optionally parameters may be provided in json or pickle format.

Run on recorded data using one of the following commands:

    LOG_LEVEL="DEBUG" dranspose replay -w "src.worker:BalderWorker" -r "src.reducer:BalderReducer" -s "src.hdf5_sources:XESSource" -p params.json

    LOG_LEVEL="INFO" dranspose replay -w "src.worker:BalderWorker" -r "src.reducer:BalderReducer" -f data/xes_streaming_receivereigerxes-ingester-d05f85ac-e444-44e9-8d96-b40d8566b6a6.cbors -p params.json

You can use the [HsdsViewer](https://gitlab.maxiv.lu.se/scisw/live-viewer/-/tree/master/hsdsviewer?ref_type=heads) to look at the live results:

    HsdsViewer http://balder-pipeline-hsds.daq.maxiv.lu.se/home/live xes/roi_sum


### Type checking

If you want to type check the code, install `mypy` and run

    MYPYPATH=../../dranspose:src mypy --strict src

If you don't have a dranspose installation in your environment, adapt the `MYPYPATH` accordingly.