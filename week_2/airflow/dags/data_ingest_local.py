import os
from datetime import datetime
import pandas as pd

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest import download_records, batch_records


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL = os.getenv('URL', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet')
TYPE = os.getenv('TYPE', 'parquet')
TABLENAME = os.getenv('TABLENAME', 'airflowtable1')


local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 23)
)

if TYPE == 'parquet':
    dataset = 'dataset.parquet'
else:
    dataset = 'dataset.csv'

with local_workflow:
    download_task = PythonOperator(
        task_id='download',
        python_callable=download_records,
        op_kwargs=dict(
            url=URL,
            file_type=TYPE
        )
    )

    batch_task = PythonOperator(
        task_id="bacth",
        python_callable=batch_records,
        op_kwargs=dict(
            file_type=TYPE,
            tablename=TABLENAME,
            dataset=dataset
        ),
    )

    download_task >> batch_task