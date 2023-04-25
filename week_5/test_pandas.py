import time
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc
from google.cloud import storage

start_time = time.time()

storage_client = storage.Client()
bucket = storage_client.get_bucket('de-zoomcamp-paul')
blob = bucket.get_blob('nyc_data.parquet')
print('Downloaded')
data = pd.read_parquet(blob.download_as_string())
print(len(data))

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Elapsed time: {elapsed_time:.2f} seconds")