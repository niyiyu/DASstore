import zarr
from minio import Minio
from datetime import datetime

from ..utils.credential import get_credential


class Client:
    def __init__(
        self,
        bucket,
        endpoint,
        region="",
        secure=False,
        credential_path="~/.dasstore/credentials",
    ):
        self.credential = get_credential(credential_path)

        self.backend = "Zarr"
        self.bucket = bucket

        self.config = {}
        self.config["region"] = region
        self.config["endpoint"] = endpoint
        self.config["key"] = self.credential["aws_access_key_id"]
        self.config["secret"] = self.credential["aws_secret_access_key"]

        if secure:
            self.config["secure"] = "https"
        else:
            self.config["secure"] = "http"

        self.minio = Minio(
            endpoint,
            self.credential["aws_access_key_id"],
            self.credential["aws_secret_access_key"],
            secure=secure,
        )

        self.get_storage_options()
        self.get_metadata()

        self._t0 = datetime.strptime(
            self.meta["acquisition.acquisition_start_time"], "%Y-%m-%dT%H:%M:%S.%f"
        )
        self._fs = self.meta["acquisition.acquisition_sample_rate"]

    def get_raw_data(self, channels, starttime, endtime, attr="RawData"):
        stime = datetime.strptime(starttime, "%Y-%m-%dT%H:%M:%S.%f")
        etime = datetime.strptime(endtime, "%Y-%m-%dT%H:%M:%S.%f")

        istart = int((stime - self._t0).total_seconds() * self._fs)
        iend = int((etime - self._t0).total_seconds() * self._fs)

        A = zarr.open(
            f"s3://{self.bucket}/RawData", "r", storage_options=self.storage_options
        )

        return A[channels, istart:iend]

    def get_metadata(self):
        A = zarr.open_array(
            f"s3://{self.bucket}/RawData", "r", storage_options=self.storage_options
        )
        self.meta = dict(A.attrs)

    def get_storage_options(self):
        self.storage_options = {
            "key": self.config["key"],
            "secret": self.config["secret"],
            "client_kwargs": {
                "endpoint_url": f"{self.config['secure']}://{self.config['endpoint']}"
            },
        }

    def _list_objects(self):
        for i in self.minio.list_objects(self.bucket):
            print(i.object_name)

    def __str__(self):
        s = ""
        s += f"Bucket:  \t s3://{self.bucket} \n"
        s += f"Exist:   \t {self.minio.bucket_exists(self.bucket)} \n"
        s += f"Endpoint:\t {self.config['secure']}://{self.config['endpoint']}\n"
        s += f"Backend: \t {self.backend}\n"

        return s

    def _repr_pretty_(self, p, cycle):  # pragma: no cover
        p.text(self.__str__())
