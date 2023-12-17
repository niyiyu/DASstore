import numpy as np
from dasstore.zarr import Client

client = Client("shared/niyiyu/dasstore-demo-zarr", "https://dasway.ess.washington.edu")
client = Client(
    "shared/niyiyu/dasstore-demo-zarr", "https://dasway.ess.washington.edu", anon=True
)
client = Client(
    "shared/niyiyu/dasstore-demo-zarr", "https://dasway.ess.washington.edu", secure=True
)
client = Client(
    "shared/niyiyu/dasstore-demo-zarr", "https://dasway.ess.washington.edu", anon=True
)
client = Client(
    "s3://shared/niyiyu/dasstore-demo-zarr",
    "https://dasway.ess.washington.edu",
    anon=True,
)

metadata = client.meta
channel = client.get_channel()

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
