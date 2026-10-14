from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("silver_clean").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("/work/data/raw/")
print("Rows read from bronze:", df.count())

df = df.withColumn("is_flex_fare", F.col("payment_type") == 0)
df = df.withColumn("invalid_trip_time", F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"))
df = df.withColumn("negative_fare", F.col("fare_amount") < 0)
df = df.withColumn("negative_total", F.col("total_amount") < 0)
df = df.withColumn("zero_distance", F.col("trip_distance") == 0)
df = df.withColumn("pickup_outside_2025", F.year("tpep_pickup_datetime") != 2025)

df = df.withColumn("pickup_month", F.date_format("tpep_pickup_datetime", "yyyy_MM"))

df.write.mode("overwrite").partitionBy("pickup_month").parquet("/work/data/silver/yellow_taxi")

silver = spark.read.parquet("/work/data/silver/yellow_taxi")
print("Rows in silver:", silver.count())

for name in ["is_flex_fare", "invalid_trip_time", "negative_fare",
             "negative_total", "zero_distance", "pickup_outside_2025"]:
    print(name, silver.filter(F.col(name)).count())

silver.groupBy("pickup_month").count().orderBy("pickup_month").show(20)