# DASstore: an object storage for Distributed Acoustic Sensing
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![DOI](https://zenodo.org/badge/566535376.svg)](https://zenodo.org/badge/latestdoi/566535376)
## Overview
This work introduce a new storage solution for distributed acoustic sensing (DAS) data. We introduce object storage that has been widely used in commercial cloud storage (AWS S3, Azure Blob, etc.) to a local data server. Instead of hosting data in the HDF5 format, we proposed hosting DAS data in the Zarr format that is optimized for cloud environment.

## Data Server (MinIO) Deployment
MinIO can run as Single-Node Single-Drive (SNSD). See documentation [here](https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-single-node-single-drive.html) for more detail. Simple deployment using Docker is shown below.
```bash
# pull MinIO image
docker pull minio/minio

# 9000: url endpoint port
# 9001: console port
docker run -p 9000:9000 -p 9001:9001 --name minio \
    -v <PATH/TO/DB> \
    -e MINIO_ROOT_USER= <ADMIN-USER> \
    -e MINIO_ROOT_PASSWORD= <ADMIN-PASSWORD> \
    -d minio/minio server /data --console-address ":9001"
```

Alternatively, users can deploy MinIO in the Single-Node Multiple-Drive mode (SNMD). Documentation of more advanced deployments can be found [here](https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-single-node-multi-drive.html).

## Data Client
We provide a Python client to conveniently query the data with a Zarr backend. The client supports either anonymous access for public bucket, or private bucket (access key and secret required).
```python
from dasstore.zarr import Client

client = Client(BUCKET, ENDPOINT_URL, anon = False, secure = False)

# get 20-minute data from one subarray (channel 500 - 1000)
client.get_data(np.arange(500, 1000),
                starttime = "2021-11-02T00:00:14.000",
                endtime   = "2021-11-03T00:20:14.005")
```

## Metadata
The metadata includes user-defined data for the DAS experiment. Here, we follow the [metadata convention](https://github.com/DAS-RCN/DAS_metadata) proposed by the DAS Research Coordination Network (DAS-RCN). There are five levels of metadata describing an experiment under this convention: Overview, Cable and Fiber, Interrogator, Acquisition, and Channel. These metadata, as key-value-pair attributes, are saved together with the raw data.

```python
# get metadata
client.get_metadata()

# get channel location (calibrated)
client.get_channel()
```

## Public data service
Public data service from UW-PSF will be available soon.

### Tutorial
Several tutorials about uploading data to the object storage using Zarr or TileDB backend is available at `/tutorials`. We will provide a Google Colab to play with a mini dataset from our storage server. 

### Schema
Updating soom.

## Reference
Links below provides useful information about UW-PSF, the data, the format and the storage. If you have more questions, feel free to contact us. 
* https://psf.uw.edu
* https://zarr.readthedocs.io/en/stable/
* https://min.io
* https://tiledb.com
* https://aws.amazon.com/s3/
