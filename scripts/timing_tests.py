import os
import time

import psutil

# TEST = "minio_tiledb"
# TEST = "minio_zarr"
# TEST = "zarr+local"
# TEST = "h5+local"
TEST = "h5chunked+local"
# TEST = "h5+crossmount"

MPIRUN = "/home/niyiyu/anaconda3/envs/dasnif/bin/mpirun"
PYTHON = "/home/niyiyu/anaconda3/envs/dasnif/bin/python"

for NPROC in [1, 2, 4, 8, 16, 32]:
    if TEST == "minio_zarr":
        for bucket in [1, 2, 3, 4, 5, 10, 50, 100, 500]:
            for _ in range(5):
                t0 = time.time()
                p0 = psutil.net_io_counters().bytes_recv
                os.system(
                    f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_minio_zarr.py --bucket {bucket}"
                )
                t = time.time() - t0
                p = (psutil.net_io_counters().bytes_recv - p0) / 1024**2
                os.system("rm -rf /tmp/tmp*")
                os.system(
                    f"echo '{bucket},{TEST},pnwstore1,siletzia,{NPROC},%.3f,%.3f' >> ../data/benchmark.csv"
                    % (t, p)
                )
                time.sleep(10)
    elif TEST == "minio_tiledb":
        for bucket in [1, 2, 3, 4, 5]:
            for _ in range(5):
                t0 = time.time()
                p0 = psutil.net_io_counters().bytes_recv
                os.system(
                    f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_minio_tiledb.py --bucket {bucket}"
                )
                t = time.time() - t0
                p = (psutil.net_io_counters().bytes_recv - p0) / 1024**2
                os.system("rm -rf /tmp/tmp*")
                os.system(
                    f"echo '{bucket},{TEST},pnwstore1,siletzia,{NPROC},%.3f,%.3f' >> ../data/benchmark.csv"
                    % (t, p)
                )
                time.sleep(10)

    elif TEST == "h5+crossmount":
        os.system(f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_crossmount_h5.py")
    elif TEST == "h5+local":
        t0 = time.time()
        os.system(f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_local_h5.py")
        t = time.time() - t0
        print("Test finished in %.3f seconds" % t)
    elif TEST == "h5chunked+local":
        for bucket in [1, 2, 3, 4, 5, 10, 50, 100, 500]:
            for _ in range(5):
                t0 = time.time()
                os.system(f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_local_h5_chunked.py --bucket {bucket}")
                t = time.time() - t0
                os.system(
                    f"echo '{bucket},{TEST},{NPROC},%.3f' >> ../data/benchmark_h5_chunked.csv" % t
                )
                print("Test finished in %.3f seconds" % t)
    elif TEST == "zarr+local":
        os.system(f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_local_zarr.py")
