gcloud dataproc jobs submit pyspark \
--properties ^#^spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0,org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.1.0 \
realtime_streaming/test_streaming.py \
--cluster spark-etl \
--region us-central1

# To delete a topic
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic user_browsing_logs

#To list the topics 
/usr/lib/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list

