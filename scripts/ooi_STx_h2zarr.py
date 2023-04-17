# OOI_STx_h2zarr.py
#   convert OOI South Tx cable data to zarr

import os

import h5py
import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm

from DASStore.modules import convert

# Download all files from RCA Server
os.system("bash batch_download.sh")

# load list of files from disk
h5dir = "/das_data/h5files/"
zarrdirr = "/das_data/zarr/ooi_South_Tx.zarr"

convert.h52zarr_xarray(h5dir, zarrdirr)
