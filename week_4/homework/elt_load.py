from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp import GcpCredentials

@task(log_prints = True, retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    print(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(tags=["load to bq"])
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom")

    df.to_gbq(
        destination_table="homework4_staging.yellow_tripdata_2020",
        project_id="sendme-test-db",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def bq_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        
    df = fetch(dataset_url)
    df_clean = clean(df)
    write_bq(df_clean)


@flow()
def bq_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = 'yellow'
):
    for month in months:
        bq_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = 'yellow'
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2020
    bq_parent_flow(months, year, color)