from dranspose.replay import replay

def test_replay():

    replay("src.worker:BalderWorker",
    "src.reducer:BalderReducer",
           None,
    "src.hdf5_sources:XESSource",
    "params.json")