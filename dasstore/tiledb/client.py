import tiledb

# from minio import Minio
# from minio.error import S3Error

from datetime import datetime

from ..utils.credential import get_credential


class Client:
    def __init__(
        self,
        bucket,
        endpoint,
        region="",
        secure=False,
        use_virtual_addressing=False,
        anon=False,
        credential_path="~/.dasstore/credentials",
    ):
        self.backend = "TileDB"
        self.bucket = bucket
        self.anon = anon

        self.config = tiledb.Config()
        self.config["vfs.s3.region"] = region
        self.config["vfs.s3.endpoint_override"] = endpoint

        if not anon:
            self.credential = get_credential(endpoint, credential_path)
            self.config["vfs.s3.aws_access_key_id"] = self.credential[
                "aws_access_key_id"
            ]
            self.config["vfs.s3.aws_secret_access_key"] = self.credential[
                "aws_secret_access_key"
            ]

        if use_virtual_addressing:
            self.config["vfs.s3.use_virtual_addressing"] = "true"
        else:
            self.config["vfs.s3.use_virtual_addressing"] = "false"

        if secure:
            self.config["vfs.s3.scheme"] = "https"
        else:
            self.config["vfs.s3.scheme"] = "http"

        self.ctx = tiledb.Ctx(self.config)

        # ========================
        # Does this really matter?
        # if not anon:
        #     self.minio = Minio(
        #         endpoint,
        #         self.credential["aws_access_key_id"],
        #         self.credential["aws_secret_access_key"],
        #         secure=secure,
        #     )
        # else:
        #     self.minio = Minio(endpoint, secure=secure)

        # try:
        #     self._bucket_exist = self.minio.bucket_exists(bucket)
        # except S3Error:
        #     raise Exception("Please check access policy.")
        # ========================

        self.get_metadata()

        self._t0 = datetime.strptime(
            self.meta["acquisition.acquisition_start_time"], "%Y-%m-%dT%H:%M:%S.%f"
        )
        self._fs = self.meta["acquisition.acquisition_sample_rate"]

    def get_data(self, channels, starttime, endtime, attr="RawData"):
        if isinstance(starttime, str):
            stime = datetime.strptime(starttime, "%Y-%m-%dT%H:%M:%S.%f")
        if isinstance(endtime, str):
            etime = datetime.strptime(endtime, "%Y-%m-%dT%H:%M:%S.%f")

        istart = int((stime - self._t0).total_seconds() * self._fs)
        iend = (
            int((etime - self._t0).total_seconds() * self._fs) - 1
        )  # multi index does not follow python index convension

        with tiledb.open(
            f"s3://{self.bucket}/RawData", "r", ctx=self.ctx, attr=attr
        ) as A:
            return A.multi_index[channels, istart:iend][attr]

    def get_metadata(self):
        with tiledb.open(f"s3://{self.bucket}/RawData", "r", ctx=self.ctx) as A:
            self.meta = dict(A.meta)

    def _list_objects(self):
        for i in self.minio.list_objects(self.bucket):
            print(i.object_name)

    def __str__(self):
        s = ""
        s += f"Bucket:  \t s3://{self.bucket} \n"
        s += f"Anonymous: \t {self.anon} \n"
        # s += f"Exist:   \t {self._bucket_exist} \n"
        s += f"Endpoint:\t {self.config['vfs.s3.scheme']}://{self.config['vfs.s3.endpoint_override']}\n"
        s += f"Backend: \t {self.backend}\n"

        return s

    def _repr_pretty_(self, p, cycle):  # pragma: no cover
        p.text(self.__str__())
