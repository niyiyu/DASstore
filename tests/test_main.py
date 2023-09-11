import numpy as np
from dasstore.zarr import Client

try:
    client = Client("seadas-december-2022", "dasway.ess.washington.edu", anon=False)
except:
    pass

try:
    client = Client("seadas-december-2022", "dasway.ess.washington.edu", secure=False)
except:
    pass

client = Client("seadas-december-2022", "dasway.ess.washington.edu", anon=True)
client = Client("s3://seadas-december-2022", "dasway.ess.washington.edu", anon=True)

print(client)

metadata = client.meta
channel = client.get_channel()

## get data test
client.get_data(np.arange(50), "2022-12-01T00:00:00", "2022-12-01T00:01:00")
client.get_data(
    np.arange(50), "2022-12-01T00:00:00.000000", "2022-12-01T00:01:00.000000"
)

try:
    client.get_data(
        np.arange(50), "2022-11-30T00:00:00.000000", "2022-11-30T00:01:00.000000"
    )
except:
    pass

try:
    client.get_data(
        np.arange(50), "2023-01-01T00:00:00.000000", "2023-01-01T00:01:00.000000"
    )
except:
    pass
