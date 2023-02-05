from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time as time

spark = SparkSession\
          .builder\
          .master("spark://192.168.1.2:7077")\
          .appName("query3-SparkSQL")\
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

tripdata_df = spark.read.parquet("hdfs://master:9000/taxi_data/*.parquet")
# Iterate 10 times in order to calculate an average time of query execution
for i in range(0,10):
  
  #start timer
  start = time.time()
  
  #insert a new column that corresponds to each 15 days of the first six months (1-13)
  tripdata_df_15days = tripdata_df.withColumn("15days",floor(dayofyear(tripdata_df.tpep_pickup_datetime)/15 + 1))
  tripdata_df_15days.createOrReplaceTempView("tripdata")
  

  
  query3 = spark.sql("""SELECT avg(Total_amount),avg(Trip_distance),15days
  from tripdata
  WHERE PULocationID != DOLocationID and month(tpep_pickup_datetime) <= 6 and year(tpep_pickup_datetime) = 2022
  GROUP BY 15days
  ORDER BY 15days
  """ )
  
  
  
  query3.collect()
  end = time.time()
  #end timer
  query_time += end - start

#calculate average
avg_query_time = query_time/10


print("Average query time:", avg_query_time)
spark.stop()
