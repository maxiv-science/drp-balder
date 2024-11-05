# TODO

## Essential

## Useful
 - [ ] add pcap energy information to saved/streamed data
     - [ ] either via reading crystalconstant
     - [ ] or via reading pcap zmq stream

## Nice to have
 - [ ] send mask to dranspose as [binary parameters](https://gitlab.maxiv.lu.se/scisw/daq-modules/dranspose/-/blob/main/tests/test_replay.py#L202) via [REST interface](https://gitlab.maxiv.lu.se/scisw/daq-modules/dranspose/-/blob/main/tests/test_parameters.py#L114)
 - [ ] write custom live viewer
 - [ ] energy scale from PCAP on y axis


## FIXME
 - [ ] fix zeros in live prj_corr and roi_sum due to misalignment
 - [ ] fix trailing zero at end of saved roi sum

# Done
 - [ ] rm eiger_names ugly hack to work with multiple eiger names
 - [ ] rm test sleeps
 - [ ] modify clemens viewer to get preview from standard streaming-receiver
