import sys

from mpi4py import MPI

sys.path.append("../")

import argparse

import numpy as np
from scipy import signal

from dasstore.tiledb import Client


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


parser = argparse.ArgumentParser()
parser.add_argument("-b", "--bucket", type=str, required=True)
args = parser.parse_args()
bucket = args.bucket

# parallelize with ompi
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# channel_index = np.linspace(1000, 47000, 501)[:-1].astype(int) # calculate peak amplitude for 500 channels
indexes = np.arange(5000)
channel_index = np.array_split(indexes, size)[rank]

client = Client(f"TileDB-OOI-DAS-{bucket}", "pnwstore1.ess.washington.edu:9000")

d = client.get_data(
    channel_index,
    starttime="2021-11-02T00:00:14.000",
    endtime="2021-11-02T00:10:14.000",
)

# some computing. demean, taper, filtering
process(d)
