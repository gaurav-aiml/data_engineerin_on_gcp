bucket="gs://gmp-etl"
cluster_name="sqoop-cluster-ephemeral"
instance_name="de-on-gcp:us-central1:sqoop-demo-instance"

gcloud dataproc clusters create $cluster_name \
--region us-central1 \
--zone us-central1-a \
--scopes default,sql-admin \
--properties hive:hive.metastore.warehouse.dir=$bucket/hive-warehouse \
--metadata=enable-cloud-sql-hive-metastore=false \
--metadata=additional-cloud-sql-instances=$instance_name=tcp:3307 \
--initialization-actions gs://gmp-etl/init-actions/cloud-sql-proxy.sh \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 10 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 10 \
--image-version 1.2


