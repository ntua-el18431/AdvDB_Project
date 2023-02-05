from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time as time


spark = SparkSession\
          .builder\
          .master("spark://192.168.1.2:7077")\
          .appName("query4-SparkSQL")\
          .getOrCreate()

#Read the data as requested
tripdata_files_path_list = ["yellow_tripdata_2022-01.parquet",
                    "yellow_tripdata_2022-02.parquet",
                    "yellow_tripdata_2022-03.parquet",
                    "yellow_tripdata_2022-04.parquet",
                    "yellow_tripdata_2022-05.parquet",
                    "yellow_tripdata_2022-06.parquet"]

tripdata_df = df = spark.read.parquet("hdfs://master:9000/taxi_data/*.parquet")
query_time = 0
total_time = 0


for i in range(0,10):
  
  
  start = time.time()
  
  
  tripdataHour= tripdata_df.withColumn("Hour",hour(tripdata_df.tpep_pickup_datetime))
  tripdataHourDay = tripdataHour.withColumn("Day",dayofweek(tripdataHour.tpep_pickup_datetime))
  tripdataHourDay.createOrReplaceTempView("tripdata")
  
  
  
  query4 = spark.sql(""" SELECT avg(Passenger_count) as help,Hour,Day
  from tripdata
  WHERE   month(tpep_pickup_datetime) <= 6 and year(tpep_pickup_datetime) = 2022 
  GROUP BY Day,Hour
  ORDER BY  DAY
  """ )
  
  
  query4.createOrReplaceTempView("temp")
  
  result = spark.sql(""" SELECT * from (
          SELECT help,Hour,Day,
          ROW_NUMBER() OVER (PARTITION BY Day order by help DESC) AS rank
          from temp)
          WHERE rank <=3
  
  """)
 
  
  result.collect()
  end = time.time()
  
  query_time += end - start
 
avg_query_time = query_time/10


print("Average query time:", avg_query_time)
spark.stop()
