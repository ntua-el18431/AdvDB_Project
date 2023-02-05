from pyspark.sql import SparkSession
import datetime, sys
from pyspark.sql.functions import *
import time as time

spark = SparkSession\
          .builder\
          .master("spark://192.168.1.2:7077")\
          .appName("query5-SparkSQL")\
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


for i in range(0,10):

  
          
  start = time.time()
  
  
  tripdataTip= tripdata_df.withColumn("Tip",(tripdata_df.tip_amount/tripdata_df.fare_amount))
  tripdataTipDay = tripdataTip.withColumn("Day",dayofmonth(tripdataTip.tpep_pickup_datetime))
  tripdataTipDay.createOrReplaceTempView("tripdata")
  
  
  
  query5 = spark.sql(""" SELECT avg(Tip) avgtip ,month(tpep_pickup_datetime) as month,Day
  from tripdata
  WHERE   month(tpep_pickup_datetime) <= 6 and year(tpep_pickup_datetime) = 2022 
  GROUP BY Day,month
  
  """ )
  query5.createOrReplaceTempView("temp")
  
  result = spark.sql(""" SELECT * from (
          SELECT avgtip,month,Day,
          ROW_NUMBER() OVER (PARTITION BY month order by avgtip DESC) AS rank
          from temp)
          WHERE rank <=5
          ORDER BY month ASC,rank ASC
  
  """)
  
  
  result.collect()
  end = time.time()
  query_time += end - start
  

avg_query_time = query_time/10


print("Average query time:", avg_query_time)
spark.stop()
 
