# DASStore: the prototype of an object storage system for Distributed Acoustic Sensing data.
## Overview
This work introduce a new storage solution for distributed acounstic sensing data. We introduced object storage that has been widely used in cloud storage (AWS S3, Azure Blob, etc.) to local data server. Instead of hosting data in HDF5 format, we proposed converting DAS data into Zarr format that is optimized for cloud environment. 

## Installation
### Data Server
```
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

### Client
```
    docker pull minio/mc
    docker run -it --rm --entrypoint=/bin/bash minio/mc
    $ mc config host add minio <ENDPOINT_URL>:<ENDPOINT_PORT> <ADMIN-USER> <ADMIN-PASSWORD> 
    Added `minio` successfully.
```