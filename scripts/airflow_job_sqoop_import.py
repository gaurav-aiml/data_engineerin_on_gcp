#main airflow import
from airflow import models, DAG

#imports to facilitate automation according to date
from datetime import datetime, timedelta, date

#imports for PySpark Job and managing the DataProc cluster for the same
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator

#imports for transfering files from GCS to BIGQuery (bq load opeartions)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

#imports to run bash script
from airflow.operators.bash_operator import BashOperator

from airflow.models import *

#imports to read environment variables set in the airflow environment
from airflow.models import Variable

#imports for the Trigger Rule ()
from airflow.utils.trigger_rule import TriggerRule

BUCKET = "gs://gmp-etl"
current_date = str(date.today())



DEFAULT_ARGS = {
    "owner":"airflow",
    "depends_on_past": True,
    "start_date":datetime.utcnow(),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":2,
    "retry_delay":timedelta(minutes=5),
    "project_id":"de-on-gcp",
    "scheduled_interval":"30 22 * * *"
}

with DAG("sqoop_full_table_import", default_args=DEFAULT_ARGS) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id="create_dataproc_cluster",
        cluster_name="ephemeral-sqoop-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-2",
        init_actions_uris=["gs://gmp-etl/init-actions/cloud-sql-proxy.sh"],
        num_workers=2,
        worker_machine_type="n1-standard-2",
        master_boot_disk_size=20,
        worker_boot_disk_size=20,
        region="us-central1",
        zone="us-central1-a",
        service_account_scopes=["https://www.googleapis.com/auth/sqlservice.admin"],
        properties={"hive:hive.metastore.warehouse.dir":"gs://gmp-etl/hive-warehouse"},
        metadata={"additional-cloud-sql-instances":"de-on-gcp:us-central1:sqoop-demo-instance=tcp:3307","enable-cloud-sql-hive-metastore":"false"}
    )

    submit_sqoop_job = BashOperator(
        task_id="sqoop_simple_import",
        bash_command="bash /home/airflow/gcs/plugins/sqoop_simple_import.sh ephemeral-sqoop-cluster-{{ds_nodash}}",
        dag=dag
    )

    bq_load_full_table = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_full_table_import",
        bucket="gmp-etl",
        source_objects=["sqoop_output/*.avro"],
        destination_project_dataset_table="de-on-gcp.flight_analysis.flight_delays_sqoop",
        auto_detect=True,
        source_format="AVRO",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        # time_partitioning={"field":"flight_date"},
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    # delete_cluster = DataprocClusterDeleteOperator(
    #     task_id="delete-dataproc-cluster",
    #     cluster_name="ephemeral-sqoop-cluster-{{ds_nodash}}",
    #     region="us-central1",
    #     trigger_rule=TriggerRule.ALL_DONE
    # )

    create_cluster.dag = dag
    create_cluster.set_downstream(submit_sqoop_job)
    submit_sqoop_job.set_downstream(bq_load_full_table)
    # bq_load_full_table.set_downstream(delete_cluster)