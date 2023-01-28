import os
import argparse
import pandas as pd
from connect import get_connection

#Getting the Data

def download_records(url, file_type):
    #Downloading the Data
    if file_type == 'parquet':
        os.system(f"curl -sSL {url} > dataset.parquet")
     
    else:
        os.system(f"curl -sSL {url} > dataset.{file_type}")


  
def batch_records(file_type, tablename, dataset):
    #Transforming the Data
    if file_type == 'parquet':
        df = pd.read_parquet(dataset)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    else:
        df = pd.read_csv(dataset)

    #This is used to make a connection to the postgress DB
    engine = get_connection()

    #Batching the records
    df.to_sql(name=tablename, con=engine, if_exists='replace')

    print('Batch Successful')

    engine.connect().close()

if __name__ == '__main__':
    batch_records()
    

    

    





