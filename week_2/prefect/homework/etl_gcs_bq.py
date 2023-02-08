from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, tags=["extract"])
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data2/")
    return Path(f"data2/{gcs_path}")


@task(tags=["load to bq"])
def write_bq(path: Path) -> None:
    """Write DataFrame to BiqQuery"""

    df = pd.read_parquet(path)

    print(len(df))

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dehomework.rides",
        project_id="gcpprojects-372909",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def homework_etl_gcs_to_bq(color: str, year: int, month: int) -> None:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    
    write_bq(path)

@flow
def homework_etq_parent_flow(months: list[int], year: int, color: str):
    
    for month in months:

        homework_etl_gcs_to_bq(color, year, month)


if __name__ == "__main__":
    color = 'yellow'
    year = '2019'
    months = [2,3] 

    homework_etq_parent_flow(months, year, color)