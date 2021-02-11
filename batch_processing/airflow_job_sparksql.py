#imports to facilitate automation according to date
from datetime import datetime, timedelta, date

#main airflow import
from airflow import models, DAG

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


current_date = str(date.today())
BUCKET = "gs://gmp-etl"
SPARK_JOB = BUCKET + "/scripts/flight_delay_analysis.py"

DEFAULT_ARGS = {
    "owner":"airflow",
    "depends_on_past": True,
    "start_date":datetime.utcnow(),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":2,
    "retry_delay":timedelta(minutes=5),
    "project_id":"de-on-gcp",
    "scheduled_interval":"15 20 * * *"
}

with DAG("flight_analysis_dag", default_args=DEFAULT_ARGS) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-2",
        num_workers=2,
        worker_machine_type="n1-standard-2",
        master_boot_disk_size=20,
        worker_boot_disk_size=20,
        region="us-central1",
        zone="us-central1-a"
    )

    submit_spark_job = DataProcPySparkOperator(
        task_id="run-spark-job",
        main=SPARK_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-central1"
    )

    bq_load_delay_by_airline = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_delay_by_ariline",
        bucket="gmp-etl",
        source_objects=["flight_analysis_outputs/"+current_date+"_delay_by_airline/*.json"],
        destination_project_dataset_table="de-on-gcp.flight_analysis.flight_delays_by_airline",
        auto_detect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        time_partitioning={"field":"flight_date"},
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    bq_load_delay_by_route = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_delay_by_route",
        bucket="gmp-etl",
        source_objects=["flight_analysis_outputs/"+current_date+"_delay_by_route/*.json"],
        destination_project_dataset_table="de-on-gcp.flight_analysis.flight_delays_by_route",
        auto_detect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        time_partitioning={"field":"flight_date"},
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    bq_load_delay_by_dist_cat = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_delay_by_dist_cat",
        bucket="gmp-etl",
        source_objects=["flight_analysis_outputs/"+current_date+"_delay_by_dist_cat/*.json"],
        destination_project_dataset_table="de-on-gcp.flight_analysis.flight_delays_by_dist_cat",
        auto_detect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        time_partitioning={"field":"flight_date"},
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete-dataproc-cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-central1",
        trigger_rule=TriggerRule.ALL_DONE
    )

    delete_transformed_files = BashOperator(
        task_id="delete_transformed_files",
        bash_command="gsutil -m rm -r "+BUCKET+"/flight_analysis_outputs/*"
    )

    create_cluster.dag = dag
    create_cluster.set_downstream(submit_spark_job)
    submit_spark_job.set_downstream([bq_load_delay_by_airline,bq_load_delay_by_dist_cat, bq_load_delay_by_route, delete_cluster])
    delete_cluster.set_downstream(delete_transformed_files)


