import time
import os

TEST = "zarr+minio"
# TEST = "h5+local"
# TEST = "h5+crossmount"

MPIRUN = "/home/niyiyu/anaconda3/envs/asdf/bin/mpirun"
PYTHON = "/home/niyiyu/anaconda3/envs/asdf/bin/python"

for NPROC in [16]:
    for _ in range(1):
        t0 = time.time()

        if TEST == "zarr+minio":
            os.system(f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_minio_zarr.py")
        elif TEST == 'h5+crossmount':
            os.system(f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_crossmount_h5.py")
        elif TEST == 'h5+local':
            os.system(f"{MPIRUN} -np {NPROC} {PYTHON} ./mpi_local_h5.py")
        t = time.time() - t0

        print("Test finished in %.3f seconds" % t)
        #os.system(f"echo '500,{TEST},pnwstore1,cascadia,{NPROC},%.3f' >> ../docs/test.csv" % t)
        
