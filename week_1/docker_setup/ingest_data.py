#!/usr/bin/env python
# coding: utf-8

import os
from time import time
import pandas as pd
from connect import get_connection

#reading nyc_taxi_data
df = pd.read_parquet('data_set.parquet')

#reading zone data
df2 = pd.read_csv('zone_data.csv')

engine = get_connection()

def push_tb1(df):
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name='taxi_data', con=engine, if_exists='replace')


def push_tb2(df):

    df.to_sql(name='zone_data', con=engine, if_exists='replace')

    
if __name__ == '__main__':
    push_tb1(df)
    push_tb2(df2)
    engine.connect().close()