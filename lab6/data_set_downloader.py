import requests
from tqdm import tqdm
import concurrent.futures

CHUNK_SIZE = 1024 # KB


def get_url(month):
  count = f"0{month}" if month < 10 else f"{month}"
  return f"https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-{count}.csv"




def download_file_for_month(month):
  url = get_url(month)
  r = requests.get(url, stream=True)
  file_size = int(r.headers["content-length"])
  with open(f"data/yellow_tripdata_2019-{month}.csv", "wb") as f:
    for data in tqdm(iterable=r.iter_content(chunk_size=CHUNK_SIZE), total=file_size/CHUNK_SIZE, unit="KB"):
      f.write(data)
  print("downloaded data set for month {month}")



with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    executor.map(download_file_for_month, range(1, 13))

