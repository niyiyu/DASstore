from mpi4py import MPI

import h5py
import numpy as np
from scipy import signal

# parallelize with ompi
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def process(d):
    sos = signal.butter(2, [0.01, 1], 'bp', fs=200, output='sos')  # bandpass filter 0.01 Hz to 1 Hz
    taper = np.hanning(120000)    # taper to use
    d = d-np.mean(d)
    d *= taper
    filtered = signal.sosfilt(sos, d)
    m = filtered.max()
    return d

indexes = np.arange(5000)
channel_index = np.array_split(indexes, size)[rank]


d = np.zeros([len(channel_index), 120000])
for i in range(10):
    # concatenate 10 minutes of data
    f = h5py.File(f"../data/hdf5/South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_2021-11-02T000{i}14Z.h5", mode = "r")
    d[:, i*12000:(i+1)*12000] = f['/Acquisition/Raw[0]/RawData'][channel_index, :]
    f.close()

# some computing. demean, taper, filtering
process(d)