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
SPARK_JOB = BUCKET + "/scripts/hive_sql_script.py"

DEFAULT_ARGS = {
    "owner":"airflow",
    "depends_on_past": False,
    "start_date":datetime(2021,2,13),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":2,
    "retry_delay":timedelta(minutes=5),
    "project_id":"de-on-gcp",
    "scheduled_interval":"*/15 * * * *"
}


with DAG("visit_by_category", default_args=DEFAULT_ARGS, catchup=True) as dag:
    submit_pyspark = DataProcPySparkOperator(
        task_id="run_hive_sql_query",
        main=SPARK_JOB,
        cluster_name="hive-cluster",
        region="us-central1"
    ) 
    submit_pyspark.dag = dag