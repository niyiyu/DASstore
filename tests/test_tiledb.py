import numpy as np
import pytest

from dasstore.tiledb import Client

buckets = [
    "shared/niyiyu/dasstore-demo-tiledb",
    "s3://shared/niyiyu/dasstore-demo-tiledb",
]
endpoints = [
    "https://dasway.ess.washington.edu",
    "http://dasway.ess.washington.edu",
    "dasway.ess.washington.edu",
]
anons = [True]
times1 = [["2021-11-02T00:00:14", "2021-11-02T00:01:14"]]
times2 = [
    ["2021-11-01T00:00:14", "2021-11-01T00:01:14"],
    ["2021-11-03T00:00:14", "2021-11-03T00:01:14"],
]


@pytest.mark.parametrize("bucket", buckets)
@pytest.mark.parametrize("endpoint", endpoints)
@pytest.mark.parametrize("anon", anons)
@pytest.mark.parametrize("time", times1)
def test_client_get_data(bucket, endpoint, anon, time):
    client = Client(bucket, endpoint, anon=anon)
    metadata = client.meta
    assert type(metadata) == dict

    data = client.get_data(np.arange(5), time[0], time[1])
    assert data.shape[0] == 5
    assert data.shape[1] == 12000


@pytest.mark.parametrize("bucket", buckets)
@pytest.mark.parametrize("endpoint", endpoints)
@pytest.mark.parametrize("time", times2)
@pytest.mark.parametrize("anon", anons)
def test_client_incorrect_time(bucket, endpoint, anon, time):
    client = Client(bucket, endpoint, anon=anon)
    with pytest.raises(ValueError):
        client.get_data(np.arange(5), time[0], time[1])
