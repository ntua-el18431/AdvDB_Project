from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time as time


spark = SparkSession\
          .builder\
          .master("spark://192.168.1.2:7077")\
          .appName("query2-SparkSQL")\
          .getOrCreate()
          
          
query_time = 0
total_time = 0


#Read the data as requested
tripdata_files_path_list = ["yellow_tripdata_2022-01.parquet",
                    "yellow_tripdata_2022-02.parquet",
                    "yellow_tripdata_2022-03.parquet",
                    "yellow_tripdata_2022-04.parquet",
                    "yellow_tripdata_2022-05.parquet",
                    "yellow_tripdata_2022-06.parquet"]

tripdata_df = df = spark.read.parquet("hdfs://master:9000/taxi_data/*.parquet")
tripdata_df.createOrReplaceTempView("tripdata")


for i in range(0,10):
 
  
  start = time.time()
  
  
  query2 = spark.sql(""" SELECT MONTH(tpep_pickup_datetime) as month,max(Tolls_amount) as tolls_payed
  FROM tripdata
  WHERE MONTH(tpep_pickup_datetime) <= 6 AND Tolls_amount > 0.0
  GROUP BY MONTH(tpep_pickup_datetime)
  ORDER BY MONTH(tpep_pickup_datetime)
  
  """ )
  
  
  query2.collect()
  end = time.time()


  query_time += end - start
 
avg_query_time = query_time/10
print("Average query time:", avg_query_time)
spark.stop()
