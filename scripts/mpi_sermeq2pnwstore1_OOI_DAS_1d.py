from mpi4py import MPI

import h5py
import zarr
import glob

# parallelize with ompi
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

flist = glob.glob("/data/data2/south-data-ejm/hdd/South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_2021-11-01T16_09_15-0700/*2021-11-02*")
flist = sorted(flist)

for idf, f in enumerate(flist):
    if idf % size == rank:
        print(f"working on {f}")
        zname = f.split("_")[-1][:-3]
        f = h5py.File(f,'r')
        z = zarr.open(f"s3://OOI-DAS/{zname}.zarr",
                       storage_options = {
                           # for public data (anonymous access):
                            "anon": True,
                            "client_kwargs": {
                               # note there is no s in http
                               "endpoint_url": "http://pnwstore1.ess.washington.edu:9000"
                           }
                       }, mode = 'w')

        zacq = z.create_group("Acquisition")
        zcustom1 = zacq.create_group("Custom")

        # Raw[0] seem not working for fsspec
        # use Raw%5B0%5B? Raw0
        zraw = zacq.create_group("Raw0")
        zrawdata = zraw.create_dataset("RawData", shape=(47500, 12000), chunks=(50, 12000), dtype='i4')
        zrawdata[:, :] = f['/Acquisition/Raw[0]/RawData'][:, :]

        zrawdatatime = zraw.create_dataset("RawDataTime", shape=(12000,),  dtype='i8')
        zrawdatatime[:] = f['/Acquisition/Raw[0]/RawDataTime'][:]

        zcustom2 = zraw.create_group("Custom")
        zgpbit = zcustom2.create_dataset("GpBits", shape=(12000,),  dtype='u1')
        zgpbit[:] = f['/Acquisition/Raw[0]/Custom/GpBits'][:]

        zgps = zcustom2.create_dataset("GpsStatus", shape=(12000,), dtype='u1')
        zgps[:] = f['/Acquisition/Raw[0]/Custom/GpsStatus'][:]

        zpps = zcustom2.create_dataset("PpsOffset", shape=(12000,), dtype='u4')
        zpps[:] = f['/Acquisition/Raw[0]/Custom/PpsOffset'][:]

        zspc = zcustom2.create_dataset("SampleCount", shape=(12000,), dtype='i8')
        zspc[:] = f['/Acquisition/Raw[0]/Custom/SampleCount'][:]

        f.close()
