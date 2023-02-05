from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time as time



spark = SparkSession\
          .builder\
          .master("spark://192.168.1.2:7077")\
          .appName("query3rdd-SparkSQL")\
          .getOrCreate()
          
#Read the data as requested
tripdata_files_path_list = ["yellow_tripdata_2022-01.parquet",
                    "yellow_tripdata_2022-02.parquet",
                    "yellow_tripdata_2022-03.parquet",
                    "yellow_tripdata_2022-04.parquet",
                    "yellow_tripdata_2022-05.parquet", 
                    "yellow_tripdata_2022-06.parquet"]

tripdata_df = spark.read.parquet("hdfs://master:9000/taxi_data/*.parquet")

# Convert Dataframes to RDD's
tripdata_rdd = tripdata_df.rdd
query_time = 0 
for i in range(0,10):
  start = time.time()
  
  def partition_to_15days(row):
    datetime = row.tpep_pickup_datetime
    days_by_15 = datetime.timetuple().tm_yday//15 + 1
    return (days_by_15, (row.trip_distance, row.total_amount,1))
    
  
  tripdata_filtered = tripdata_rdd.filter(lambda x: x.DOLocationID != x.PULocationID)
  tripdata_filtered_sums = tripdata_filtered.map(partition_to_15days).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]))
  tripdata_avg = tripdata_filtered_sums.mapValues(lambda z:( z[0]/z[2], z[1]/z[2]))
  
  tripdata_avg.collect()
  
  end = time.time()

  query_time += end - start
print("Average query_time is:", query_time/10)
spark.stop()