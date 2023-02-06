import os
import argparse
import pandas as pd
from pgconnect import get_connection
from prefect import flow, task
from prefect.tasks import task_input_hash
from time import time
from datetime import timedelta

#Getting the Data
@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_records(url, file_type):
    #Downloading the Data
    if file_type == 'parquet':
        os.system(f"curl -sSL {url} > dataset.parquet")
        return 'dataset.parquet'
     
    else:
        os.system(f"curl -sSL {url} > dataset.{file_type}")
        return 'dataset.csv'

#Transforming the Data
@task(log_prints=True, retries=3)
def transform_data(dataset, file_type):
    if file_type == 'parquet':
        df = pd.read_parquet(dataset)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        return df
    else:
        print('Transforming the Data')
        df = pd.read_csv(dataset)
        return df

#Batching the Data
@task(log_prints=True, retries=3)  
def batch_records(df, tablename):
    
    #This is used to make a connection to the postgress DB
    engine = get_connection()

    #Batching the records
    df.to_sql(name=tablename, con=engine, if_exists='replace')

    print('Batch Successful')

    engine.connect().close()


@flow(name="Ingest Flow")
def main():
    URL = 'wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
    TYPE = 'csv'
    TABLENAME = 'csvtable2'

    dataset = download_records(URL, TYPE)
    dataframe = transform_data(dataset, TYPE)
    batch_records(dataframe, TABLENAME)

if __name__ == '__main__':
    main()
    

    

    





