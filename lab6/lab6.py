# INCOMPLETE:

# import numpy as np
# import h5py
# import os

# arr = np.random.randn(1000000000)

# with h5py.File('random.hdf5', 'w') as f:
#   dset = f.create_dataset("default", data=arr)

# f = h5py.File(os.path.join('random.hdf5'), mode='r')
# dset = f['default']


# sums = []

# for i in range(0, 1000000000, 1000000):
#   chunk = dset[i:i+1000000]
#   sums.append(chunk.sum())

# total = sum(sums)
# total


"""initializing a cluster and client for dask"""
# from dask.distributed import Client, LocalCluster


# cluster = LocalCluster(
#   n_workers=8,
#   dashboard_address=8787,
#   worker_dashboard_address=8788,
#   )


# client = Client(cluster)

"""dask array"""
# import dask.array as da

# x = da.random.normal(10, 0.1, size=(20000, 20000), chunks=(1000, 1000))
# y = x.mean(axis=0)
# y = x.mean(axis=0)[::100]
# y.compute()


"""dask delayed"""
from time import sleep
from dask import delayed
def inc(x):
  sleep(1)
  return x+1

def add(x, y):
  sleep(1)
  return x+y

x = delayed(inc)(10)
y = delayed(inc)(10)

z = delayed(add)(x, y)


z.compute() #22
