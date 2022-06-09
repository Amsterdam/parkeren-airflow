from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from dags.garageparkeren.common import SparkJob, add_job_to_node

ARGS = {
    "owner": "garageparkeren",
    "description": "",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

DAG_ID = "garageparkeren-to-csv"

INTERVAL = None

with DAG(
    DAG_ID,
    schedule_interval=INTERVAL,
    default_args=ARGS,
    catchup=False,
    max_active_runs=1,
) as dag:
    start = datetime.now()
    timestamp_str = start.strftime("%Y%m%d%H")

    start = DummyOperator(task_id="start", dag=dag)

    to_csv_jobs = [
        SparkJob(
            job="to-csv-bezetting-per-kwartier",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/datamart_to_csv/bezetting_per_kwartier_to_csv.py",
            spark_executor_cores=2,
        ),
        SparkJob(
            job="to-csv-opbrengst-per-kwartier",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="src/jobs/datamart_to_csv/opbrengst_per_kwartier_to_csv.py",
            spark_executor_cores=2,
        ),
        SparkJob(
            job="to-csv-parkeerduur-per-kwartier",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/datamart_to_csv/parkeerduur_per_kwartier_to_csv.py",
            spark_executor_cores=2,
        ),
        SparkJob(
            job="to-csv-dimensions",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/datamart_to_csv/dimensions_to_csv.py",
            spark_executor_cores=2,
        ),
    ]

    end_to_csv = DummyOperator(task_id="end_to_csv", dag=dag)

    for to_csv_job in to_csv_jobs:
        add_job_to_node(start, to_csv_job, timestamp_str, end_to_csv)
