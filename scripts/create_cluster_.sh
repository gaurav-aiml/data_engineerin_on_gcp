#Command to create a DataProc cluster to use SparkSQL
gcloud dataproc clusters create batch-etl \
--region us-central1 \
--zone us-central1-a \
--scopes default \
--initialization-actions gs://gmp-etl/kafka.sh \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 20 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 20 \
--image-version 1.3-debian10 \
--optional-components ANACONDA,ZOOKEEPER,JUPYTER