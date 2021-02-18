

#----------------------------------------- SPARK --------------------------------------------------------------


gcloud dataproc jobs submit pyspark \
--properties ^#^spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-streaming-kafka-0-10-assembly_2.11:2.4.5,org.apache.spark:spark-avro_2.11:2.4.5 \
realtime_streaming/py_scripts/raw_data_gcs_dump.py \
--cluster spark-etl \
--region us-central1





#----------------------------------------- KAFKA --------------------------------------------------------------

# To delete a topic
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic user_browsing_logs

#To list the topics 
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list

#To subscribe to kafka topics from the terminal
/usr/lib/kafka/bin/kafka-console-consumer.sh --bootstrap-server spark-etl-w-1:9092 --topic user_browsing_logs





#----------------------------------------- HIVE --------------------------------------------------------------

gcloud dataproc jobs submit hive \
--cluster hive-cluster \
--region us-central1 \
--execute "CREATE EXTERNAL TABLE raw_data_logs (category STRING,date_time timestamp,type STRING,pid INTEGER,state STRING,sub_cat STRING, ip_address STRING)
STORED AS PARQUET
LOCATION 'gs://gmp-etl/raw_data';"


gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region us-central1 \
    --execute "SELECT COUNT(*) from raw_data_logs;"


gcloud dataproc jobs submit pyspark \
realtime_streaming/py_scripts/hive_sql_script.py \
--cluster hive-cluster \
--region us-central1