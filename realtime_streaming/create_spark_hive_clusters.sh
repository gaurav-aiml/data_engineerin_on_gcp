gcloud dataproc clusters create spark-etl \
--region us-central1 \
--zone us-central1-a \
--scopes default \
--initialization-actions gs://gmp-etl/init-actions/kafka.sh \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 100 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 100 \
--image-version 1.4-debian10 \
--optional-components ZOOKEEPER,ANACONDA,JUPYTER &&

gcloud dataproc clusters create hive-cluster \
--region us-central1 \
--zone us-central1-a \
--scopes default \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 100 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 100 \
--image-version 1.4-debian10