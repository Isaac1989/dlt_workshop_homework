# flake8: noqa
import os
import shutil

from typing import Iterator

import dlt
from dlt.sources import TDataItems
from dlt.sources.filesystem import FileItemDict, filesystem, readers, read_parquet

import urllib.request
from concurrent.futures import ThreadPoolExecutor


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)] 
DOWNLOAD_DIR = "./donwloaded_files"

CHUNK_SIZE = 8 * 1024 * 1024  

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None
    



def stream_and_merge_parquet() -> None:
    """Demonstrates how to scan folder with parquet files, load them in chunk and merge on date column with the previous load"""
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_parquet",
        destination='bigquery',
        dataset_name="ny_taxi_data",
        progress='alive_progress'
    )
    # met_data contains 3 columns, where "date" column contain a date on which we want to merge
    # load all parquets in A801
    met_files = readers(bucket_url=DOWNLOAD_DIR, file_glob="**/*.parquet").read_parquet()
    # tell dlt to merge on date
    met_files.apply_hints(write_disposition="merge", merge_key="tpep_pickup_datetime")
    # NOTE: we load to met_parquet table
    load_info = pipeline.run(met_files.with_name("yellow_tripdata_2024"))
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)

if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))
    print("Loading files to bigquery now...")
    stream_and_merge_parquet()
    print("Done! Loading is successful...")
    shutil.rmtree(DOWNLOAD_DIR)
    
    