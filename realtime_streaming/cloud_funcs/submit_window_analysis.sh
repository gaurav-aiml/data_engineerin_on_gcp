#!/bin/sh

FUNCTION="update_cart_window_analysis"
BUCKET="gs://gmp-etl"

gcloud functions deploy ${FUNCTION} \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize