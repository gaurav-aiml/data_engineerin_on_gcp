from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import datetime
import time

conf = SparkConf().setAppName("User_Cart_Logs").setMaster("yarn")
sc = SparkContext(conf=conf)
sqlcon = SQLContext(sc)
spark = SparkSession.builder.appName("user-cart-streaming-analysis").getOrCreate()


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "spark-etl-w-1:9092") \
    .option("subscribe","user_browsing_logs") \
    .load() \
    .selectExpr("CAST(value as STRING)")

schema = StructType (
    [
        
        StructField("category",StringType(),True),
        StructField("date",TimestampType(),True),
        StructField("type",StringType(),True),
        StructField("pid",IntegerType(),True),
        StructField("state",StringType(),True),
        StructField("sub_category",StringType(),True),
        StructField("ip_address",StringType(),True)
    ]
)

df_parsed = df.select("value")
df_streaming_visits = df_parsed.withColumn("data",from_json("value",schema)).select(col("data.*"))

df_agg = df_streaming_visits \
        .where("pid is not NULL") \
        .groupBy(
            df_streaming_visits.category,
            df_streaming_visits.type,
            df_streaming_visits.state) \
        .count()

query = df_agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()




