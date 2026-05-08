from mpi4py import MPI

import h5py
import tiledb

import glob
from datetime import datetime
from tqdm import tqdm
# parallelize with ompi
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

flist = glob.glob("/data/data2/south-data-ejm/hdd/South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_2021-11-01T16_09_15-0700/*2021-11-02*")
flist = sorted(flist)

rootpath = '/data/data3/seadasn_2022-10-07_2023-01-13/'
flist = sorted(glob.glob(rootpath + "/seadasn_2022-12-*"))

# PNWstore1
# Create a configuration object
config = tiledb.Config()

# Set configuration parameters
config["vfs.s3.scheme"] = "http"
config["vfs.s3.region"] = ""
config["vfs.s3.endpoint_override"] = ""
config["vfs.s3.use_virtual_addressing"] = "false"
config["vfs.s3.aws_access_key_id"] = ""
config["vfs.s3.aws_secret_access_key"] = ""
config["sm.consolidation.mode"] = "fragment_meta"
config["sm.vacuum.mode"] = "fragment_meta"

# Create contex
ctx = tiledb.Ctx(config)

bucket = f"s3://seadas-december-2022-tiledb/"

with tiledb.open(f"{bucket}/RawData", 'r', ctx = ctx) as A:
    t0 = datetime.fromisoformat(dict(A.meta)['acquisition.acquisition_start_time']).timestamp()

for idx, i in tqdm(enumerate(flist), total = len(flist)):
    if idx % size == rank:
        try:
            f = h5py.File(i, 'r')
            
            tsp_start = int(f['/Acquisition/Raw[0]/RawDataTime'][0]/10000)/1e2 + 28800
            tsp_end = int(f['/Acquisition/Raw[0]/RawDataTime'][-1]/10000)/1e2 + 28800

            ind_start = int((tsp_start - t0)/0.01)
            ind_end = int((tsp_end - t0)/0.01)

            assert (ind_start % 6000, ind_end % 6000) == (0, 5999)
            assert f['/Acquisition/Raw[0]/RawData'].shape == (6000, 2089)
            
            with tiledb.open(f"{bucket}/RawData", 'w', ctx = ctx) as A:
                A[:, ind_start : ind_end + 1] = f['/Acquisition/Raw[0]/RawData'][:, :].T
            
        except:
            print(i)
            
        finally:
            f.close()
