# DASStore: the prototype of an object storage system for Distributed Acoustic Sensing data
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
## Overview
This work introduce a new storage solution for distributed acounstic sensing data. We introduce object storage that has been widely used in cloud storage (AWS S3, Azure Blob, etc.) to a local data server. Instead of hosting data in the HDF5 format, we proposed hosting DAS data in the Zarr format that is optimized for cloud environment. 

## Installation
### Data Server
MinIO can run in the Single-Node Single-Drive mode (SNSD). See documentation [here](https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-single-node-single-drive.html) for more detail.
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

Alternatively, users can deploy MinIO in the Single-Node Multiple-Drive mode (SNMD). See documentation [here](https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-single-node-multi-drive.html) for more detail.

## Schema

## Metadata

## Reference
* https://zarr.readthedocs.io/en/stable/
* https://min.io
* https://tiledb.com
* https://aws.amazon.com/s3/