# OOI_STx_h2zarr.py
#   convert OOI South Tx cable data to zarr

import xarray as xr
import pandas as pd
import numpy as np
import os
import h5py
from tqdm import tqdm

from dask.distributed import Client

#client = Client()
#print(client)

# setup access to Azure Storage
storage_options = {'account_name':'dasdata', 'account_key':os.environ['AZURE_KEY_dasdata']}


baseurl = url = 'http://piweb.ooirsn.uw.edu/das/data/Optasense/SouthCable/TransmitFiber/South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_2021-11-01T16_09_15-0700/'

with open('file_list.txt') as f:
    fs = f.readlines()
    
files = []

for f in fs:
    files.append(f[:-1])

first_loop = True

for file in tqdm(files, position=0, leave=True):
    print('download file...', end="\r")
    os.system(f'aria2c -q {baseurl + file}')
    
    print('read data into memory and construct xr.Dataset ...', end="\r")
    hf = h5py.File(file)
    
    ds = xr.Dataset({
        'RawData':((['distance', 'time'], hf['Acquisition']['Raw[0]']['RawData'][:])),
        'RawDataTime':('time', hf['Acquisition']['Raw[0]']['RawDataTime'][:]),
        'GpBits':('time', hf['Acquisition']['Raw[0]']['Custom']['GpBits']),
        'GpsStatus':('time', hf['Acquisition']['Raw[0]']['Custom']['GpsStatus']),
        'PpsOffset':('time', hf['Acquisition']['Raw[0]']['Custom']['PpsOffset']),
        'SampleCount':('time', hf['Acquisition']['Raw[0]']['Custom']['SampleCount'])}
    )
    
    print('write to zarr...                                                        ', end="\r")
    # create new zarr store if beginning of loop otherwize, append in time dimension
    # if first_loop:
    #    first_loop = False
    #    ds.to_zarr('abfs://zarr/SouthCable_Tx.zarr', storage_options=storage_options, mode='w-')
    #else:
    ds.to_zarr('abfs://zarr/SouthCable_Tx.zarr', storage_options=storage_options, append_dim='time')
    
    # delete downloaded file
    os.system(f'rm {file}')