from mpi4py import MPI

import zarr
import s3fs
import numpy as np
from scipy import signal
from tqdm import tqdm

# parallelize with ompi
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

channel_index = np.linspace(1000, 47000, 501)[:-1].astype(int) # calculate peak amplitude for 500 channels
sos = signal.butter(2, [0.01, 1], 'bp', fs=200, output='sos')  # bandpass filter 0.01 Hz to 1 Hz
taper = np.hanning(120000)                 # taper to use


for idx, icha in enumerate(channel_index):
    if rank == 0:
        print(idx, flush = True)
    if idx % size == rank:
        d = np.zeros(120000)
        for i in range(10):
            # concatenate 10 minutes of data
            z = zarr.open_group(f"simplecache::s3://OOI-DAS-10/2021-11-02T000{i}14Z.zarr", 
                        storage_options = {"s3":{
                                # for public data (anonymous access):
                                "anon": True,
                                "client_kwargs": {
                                    "endpoint_url": "http://pnwstore1.ess.washington.edu:9000"
                                }
                            }
                        }, mode = "r")

            # s3 = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": "http://pnwstore1.ess.washington.edu:9000"})
            # store = s3fs.S3Map(root=f'OOI-DAS-100/2021-11-02T000{i}14Z.zarr', s3=s3, check=False)
            # z = zarr.open_consolidated(store)  

            d[i*12000:(i+1)*12000] = z['/Acquisition/Raw0/RawData'][icha, :]

        # some computing. demean, taper, filtering
        d = d-np.mean(d)
        d *= taper
        filtered = signal.sosfilt(sos, d)
        m = filtered.max()
        # print(f"rank {rank}\t| {idx}\t| max: %.3f" % filtered.max(), flush = True)
