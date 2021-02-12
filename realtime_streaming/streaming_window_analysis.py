from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import datetime
import time

bucket_name="gs://gmp-etl/"


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
        StructField("datetime",TimestampType(),True),
        StructField("type",StringType(),True),
        StructField("pid",IntegerType(),True),
        StructField("state",StringType(),True),
        StructField("sub_category",StringType(),True),
        StructField("ip_address",StringType(),True)
    ]
)

df_parsed = df.select("value")
df_streaming_visits = df_parsed.withColumn("data",from_json("value",schema)).select(col("data.*"))

df_tumbling_window = df_streaming_visits \
                    .where("pid is not null") \
                    .withWatermark("datetime","300 seconds") \
                    .groupBy(
                        window(df_streaming_visits.datetime,"300 seconds"),
                        df_streaming_visits.category, 
                        df_streaming_visits.type, 
                        df_streaming_visits.state) \
                    .agg(collect_list(col('pid')).alias("pids_incart"),count(col('pid')).alias("product_count"))


def process_batch(df, epoch_id):
    df_final = df.selectExpr("window.start as start_time", "window.end as end_time", "category", 
                            "type", "state", "pids_incart", "product_count")
    df_final.show(10, False)
    df_final.coalesce(1).write \
    .format("avro") \
    .mode("append") \
    .option("checkpointLocation",bucket_name+"spark_checkpoints/") \
    .option("path", bucket_name+"aggregated_data/") \
    .save()


query = (
        df_tumbling_window.writeStream.trigger(processingTime="100 seconds") \
        .foreachBatch(process_batch) \
        .outputMode("complete") \
        .start()
)

query.awaitTermination()


