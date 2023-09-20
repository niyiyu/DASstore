import numpy as np
from dasstore.zarr import Client

client = Client("niyiyu/dasstore-demo-zarr", "dasway.ess.washington.edu", anon=True)
client = Client("niyiyu/dasstore-demo-zarr", "dasway.ess.washington.edu", secure=False)
client = Client("niyiyu/dasstore-demo-zarr", "dasway.ess.washington.edu", anon=True)
client = Client(
    "s3://niyiyu/dasstore-demo-zarr", "dasway.ess.washington.edu", anon=True
)

metadata = client.meta

## get data test
client.get_data(np.arange(50), "2021-11-02T00:00:14", "2021-11-02T00:01:14")
client.get_data(
    np.arange(50), "2021-11-02T00:00:14.000000", "2021-11-02T00:01:14.000000"
)

try:
    client.get_data(np.arange(50), "2021-11-01T00:00:14", "2021-11-01T00:01:14")
except ValueError:
    pass

try:
    client.get_data(np.arange(50), "2021-11-03T00:00:14", "2021-11-03T00:01:14")
except ValueError:
    pass
