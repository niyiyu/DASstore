from datetime import datetime

import logging

import tiledb
import pandas as pd

from ..utils.credential import get_credential


class Client:
    def __init__(
        self,
        bucket,
        endpoint,
        region="",
        use_virtual_addressing=False,
        anon=False,
        role_assigned=False,
        credential_path="~/.dasstore/credentials",
    ):
        self.backend = "TileDB"
        if "s3://" in bucket:
            self.bucket = bucket.replace("s3://", "")
        else:
            self.bucket = bucket
        self.anon = anon
        self.role_assigned = role_assigned
        self.use_virtual_addressing = use_virtual_addressing

        self.config = tiledb.Config()
        self.config["vfs.s3.region"] = region
        self.config["vfs.s3.endpoint_override"] = endpoint.rstrip("/")

        if "https://" in endpoint:
            self.config["vfs.s3.scheme"] = "https"
        elif "http://" in endpoint:
            self.config["vfs.s3.scheme"] = "http"
        else:
            self.config["vfs.s3.endpoint_override"] = (
                "https://" + self.config["vfs.s3.endpoint_override"]
            )
            logging.warning(
                f'Endpoint overwrited: {self.config["vfs.s3.endpoint_override"]}'
            )

        if self.role_assigned:
            pass
        elif not anon:
            self.credential = get_credential(endpoint, credential_path)
            self.config["vfs.s3.aws_access_key_id"] = self.credential[
                "aws_access_key_id"
            ]
            self.config["vfs.s3.aws_secret_access_key"] = self.credential[
                "aws_secret_access_key"
            ]
        else:
            # supported at tiledb==0.23.0
            self.config["vfs.s3.no_sign_request"] = "true"

        if self.use_virtual_addressing:
            self.config["vfs.s3.use_virtual_addressing"] = "true"
        else:
            self.config["vfs.s3.use_virtual_addressing"] = "false"

        self.ctx = tiledb.Ctx(self.config)

        self.storage_options = self.get_storage_options()
        self.meta = self.get_metadata()

        self._t0 = datetime.strptime(
            self.meta["acquisition.acquisition_start_time"], "%Y-%m-%dT%H:%M:%S.%f"
        )
        self._t1 = datetime.strptime(
            self.meta["acquisition.acquisition_end_time"], "%Y-%m-%dT%H:%M:%S.%f"
        )
        self._fs = self.meta["acquisition.acquisition_sample_rate"]

        for i in self.__str__().split("\n"):
            logging.info(i)

    def get_data(self, channels, starttime, endtime, attr="RawData"):
        if isinstance(starttime, str):
            if "." in starttime:
                starttime = datetime.strptime(starttime, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                starttime = datetime.strptime(starttime, "%Y-%m-%dT%H:%M:%S")
        if isinstance(endtime, str):
            if "." in endtime:
                endtime = datetime.strptime(endtime, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                endtime = datetime.strptime(endtime, "%Y-%m-%dT%H:%M:%S")

        try:
            assert starttime >= self._t0
        except AssertionError:
            raise ValueError(
                f"starttime [{starttime}] earlier than acquisition start time [{self._t0}]"
            )

        try:
            assert endtime <= self._t1
        except AssertionError:
            raise ValueError(
                f"endtime [{endtime}] later than acquisition end time [{self._t1}]"
            )

        istart = int((starttime - self._t0).total_seconds() * self._fs)
        iend = (
            int((endtime - self._t0).total_seconds() * self._fs) - 1
        )  # multi index does not follow python index convension

        with tiledb.open(
            f"s3://{self.bucket}/RawData", "r", ctx=self.ctx, attr=attr
        ) as A:
            return A.multi_index[channels, istart:iend][attr]

    def get_channel(self):
        return pd.read_csv(
            f"s3://{self.bucket}/cable.csv", storage_options=self.storage_options
        )

    def get_metadata(self):
        with tiledb.open(f"s3://{self.bucket}/RawData", "r", ctx=self.ctx) as A:
            return dict(A.meta)

    def get_storage_options(self):
        storage_options = {
            "client_kwargs": {
                "endpoint_url": f"{self.config['vfs.s3.endpoint_override']}"
            }
        }
        if not self.role_assigned:
            if self.anon:
                storage_options["anon"] = True
            else:
                storage_options["key"] = self.config["vfs.s3.aws_access_key_id"]
                storage_options["secret"] = self.config["vfs.s3.aws_secret_access_key"]
        return storage_options

    def __str__(self):
        s = ""
        s += f"Endpoint:  \t {self.config['vfs.s3.endpoint_override']}\n"
        s += f"Path:      \t s3://{self.bucket} \n"
        s += f"Anonymous: \t {self.anon} \n"
        s += f"Backend:   \t {self.backend}"

        return s

    def _repr_pretty_(self, p, cycle):  # pragma: no cover
        p.text(self.__str__())
