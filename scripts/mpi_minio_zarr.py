from mpi4py import MPI

import zarr
import numpy as np
from scipy import signal

# parallelize with ompi
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

channel_index = np.arange(1000, 1500)  # calculate peak amplitude for 500 channels
sos = signal.butter(2, [0.01, 1], 'bp', fs=200, output='sos')  # bandpass filter 0.01 Hz to 1 Hz
taper = np.hanning(120000)                 # taper to use


for idx in channel_index:
    if idx % size == rank:
        d = np.zeros(120000)
        for i in range(10):
            # concatenate 10 minutes of data
            z = zarr.open(f"s3://OOI-DAS/2021-11-02T033{i}14Z.zarr", 
                        storage_options = {
                                # for public data (anonymous access):
                                "anon": True,
                                "client_kwargs": {
                                    "endpoint_url": "http://pnwstore1.ess.washington.edu:9000"
                            }
                        }, mode = "r")
            d[i*12000:(i+1)*12000] = z['/Acquisition/Raw0/RawData'][idx, :]

        # some computing. demean, taper, filtering
        d = d-np.mean(d)
        d *= taper
        filtered = signal.sosfilt(sos, d)
        m = filtered.max()
        # print(f"rank {rank}\t| {idx}\t| max: %.3f" % filtered.max(), flush = True)