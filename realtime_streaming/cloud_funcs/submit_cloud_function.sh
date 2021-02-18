#!/bin/sh

FUNCTION="upload_hive_sql_to_bq"
BUCKET="gs://gmp-etl"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize