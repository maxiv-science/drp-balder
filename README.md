## Development

The worker and reducer may be developed locally by replaying ingester recorded streams.

Provide the classes for the worker and reducer as well as the ingester files.
Optionally parameters may be provided in json or pickle format.

Run on recorded data using the following command:

```LOG_LEVEL="DEBUG" dranspose replay -w "src.worker:BalderWorker" -r "src.reducer:BalderReducer" -s "src.hdf5_sources:XESSource"  -p params.json ```
