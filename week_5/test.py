import pyspark
from pyspark.sql import SparkSession
import os

def test_spark():
    """This creates a temporary spark session, and kills it once the process exists"""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .csv('taxi+_zone_lookup.csv')

    df.show()

   # df.write.parquet('zones')

if __name__ == '__main__':
    os.system("wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz -O fhvhv_tripdata_2021-01.csv ")
    test_spark()
