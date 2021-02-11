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

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()




