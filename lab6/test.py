# %%time
from dask import delayed, compute
import pandas as pd
from dask.distributed import Client

client = Client(n_workers = 4)

PAR_SUM = []
PAR_COUNT = []

def calc_mean_parallel(count):
  global PAR_COUNT
  global PAR_SUM
  path = f"data/yellow_tripdata_2019-{count}.csv"
  print('loading file', count)
  data = delayed(pd.read_csv)(path)
  PAR_COUNT.append(delayed(len)(data.index))
  PAR_SUM.append(delayed(data.tip_amount.sum)())

print("calculating mean parallelly")
for i in range(1, 2):
  calc_mean_parallel(i)

PAR_SUM, PAR_COUNT = compute(PAR_SUM, PAR_COUNT)
total_sum = sum(PAR_SUM)
total_count = sum(PAR_COUNT)

def get_mean(sum, count):
  return sum/count

mean = get_mean(total_sum, total_count)
print("calculated mean = ", mean)
client.close()