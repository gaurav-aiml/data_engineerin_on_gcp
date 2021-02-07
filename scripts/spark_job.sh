gcloud dataproc jobs submit pyspark \
gs://gmp-etl/scripts/flight_delay_analysis.py \
--cluster batch-etl \
--region us-central1