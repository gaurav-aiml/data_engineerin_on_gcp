from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import datetime
import time

bucket_name="gs://gmp-etl/"


conf = SparkConf().setAppName("User_Cart_Logs").setMaster("yarn")
#changed from yarn-client to yarn
sc = SparkContext(conf=conf)
sqlcon = SQLContext(sc)
spark = SparkSession.builder.appName("user-cart-streaming-analysis").getOrCreate()


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "spark-etl-w-1:9092") \
    .option("subscribe","user_browsing_logs") \
    .option("failOnDataLoss","false") \
    .load() \
    .selectExpr("CAST(value as STRING) ")

schema = StructType(
    [
        StructField("category",StringType(),True),
        StructField("date_time",TimestampType(),True),
        StructField("type",StringType(),True),
        StructField("pid",IntegerType(),True),
        StructField("state",StringType(),True),
        StructField("sub_category",StringType(),True),
        StructField("ip_address",StringType(),True)
    ]
)

df_parsed = df.select("value")

df_main = df_parsed.withColumn("data", from_json("value",schema)).select(col('data.*'))

def process_batch(df, epoch_id):

    df.show(10,False)

    df.coalesce(1).write \
    .format("parquet") \
    .mode("append") \
    .option("checkpointLocation",bucket_name+"/raw_data_checkpoints/") \
    .option("path",bucket_name+"/raw_data/") \
    .save()


query = (
        df_main.writeStream.trigger(processingTime="60 seconds") \
        .foreachBatch(process_batch) 
        .outputMode("append") 
        .start()
)

query.awaitTermination()