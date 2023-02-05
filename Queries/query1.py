from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time as time

spark = SparkSession\
          .builder\
          .master("spark://192.168.1.2:7077")\
          .appName("query1-SparkSQL")\
          .getOrCreate()


#Read the data as requested
tripdata_files_path_list = ["yellow_tripdata_2022-01.parquet",
                    "yellow_tripdata_2022-02.parquet",
                    "yellow_tripdata_2022-03.parquet",
                    "yellow_tripdata_2022-04.parquet",
                    "yellow_tripdata_2022-05.parquet",
                    "yellow_tripdata_2022-06.parquet"]

tripdata_df = df = spark.read.parquet("hdfs://master:9000/taxi_data/*.parquet")
taxi_lookup_df = spark.read.csv("hdfs://master:9000/taxi_data/taxi+_zone_lookup.csv",header = True)
tripdata_df.createOrReplaceTempView("tripdata")
taxi_lookup_df.createOrReplaceTempView("taxilookup")

query_time = 0
total_time = 0

#Iterate over 10 to calculate an average time of execution
for i in range(0,10):  
   
  
  start = time.time()


  joined_df = tripdata_df.join(taxi_lookup_df, tripdata_df.DOLocationID == taxi_lookup_df.LocationID, "inner")
  Filtered = joined_df.filter((month(col("tpep_pickup_datetime")) == 3) & (col("Zone") == "Battery Park"))
  Maxtip = Filtered.select(max(Filtered['Tip_amount']).alias('maxtip'))
  Result = Filtered.filter(Filtered['Tip_amount'] == Maxtip.first()['maxtip'])


  Result.collect()
  end = time.time()
  query_time += (end - start)


print("Average query time:", query_time/10)

spark.stop()
