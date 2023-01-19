import os
import argparse
import pandas as pd
from connect import get_connection



#Getting the Data

def batch_records():
    """This function takes in the link to the unique download path
    of the parquet file, and batches the record to postgres"""

    parser = argparse.ArgumentParser()

    parser.add_argument('--url', type=str, help='Input the url link for download')

    parser.add_argument('--filetype', type=str, help='Select either csv or Parquet File')

    parser.add_argument('--tablename', type=str, help='The name of the table you want to create')

    known_args = parser.parse_args()

    url = known_args.url
    table_name = known_args.tablename
    file_type = known_args.filetype

    #Downloading the Data
    if file_type == 'parquet':
        os.system(f"wget {url} -O dataset.parquet")
        df = pd.read_parquet('dataset.parquet')
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    else:
        os.system(f"wget {url}-O dataset.csv")
        df = pd.read_csv('dataset.csv')

    print(df.head(10))
    #Batching the records to postgress
    
    #This is used to make a connection to the postgress DB
    engine = get_connection()

    df.to_sql(name=table_name, con=engine, if_exists='replace')

    print('Batch Successful')

if __name__ == '__main__':
    batch_records()
    engine.connect().close()

    

    





