import hdf5plugin
import h5py
import numpy as np
from mpi4py import MPI
from scipy import signal
import argparse

# parallelize with ompi
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

parser = argparse.ArgumentParser()
parser.add_argument("-b", "--bucket", type=str, required=True)
args = parser.parse_args()
bucket = args.bucket

def process(d):
    sos = signal.butter(
        2, [0.01, 1], "bp", fs=200, output="sos"
    )  # bandpass filter 0.01 Hz to 1 Hz
    taper = np.hanning(120000)  # taper to use
    d = d - np.mean(d)
    d *= taper
    filtered = signal.sosfilt(sos, d)
    m = filtered.max()
    return d

indexes = np.arange(5000)
channel_index = np.array_split(indexes, size)[rank]

d = np.zeros([len(channel_index), 120000])
# concatenate 10 minutes of data
f = h5py.File(
    f"../data/chunked_h5/chunked_{bucket}.h5",
    mode="r",
)
d = f["/RawData"][channel_index, :]
f.close()

# some computing. demean, taper, filtering
process(d)
