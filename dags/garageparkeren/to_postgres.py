from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from dags.garageparkeren.common import SparkJob, add_job_to_node

ARGS = {
    "owner": "garageparkerenraw - thomask",
    "description": "",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

DAG_ID = "garageparkerenraw-to-postgres"

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

    to_postgres_jobs = [
        SparkJob(
            job="to-postgres-bezetting-per-kwartier",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/minio_to_postgres/bezetting_per_kwartier.py",
            spark_executor_cores=2,
        ),
        SparkJob(
            job="to-postgres-opbrengst-per-kwartier",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/minio_to_postgres/opbrengst_per_kwartier.py",
            spark_executor_cores=2,
        ),
        SparkJob(
            job="to-postgres-parkeerduur-per-kwartier",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/minio_to_postgres/parkeerduur_per_kwartier.py",
            spark_executor_cores=2,
        ),
        SparkJob(
            job="to-postgres-load-jdbc-dimension",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/minio_to_postgres/load_jdbc_dimension.py",
            spark_executor_cores=2,
        ),
        SparkJob(
            job="to-postgres-bezetting-per-kwartier-kaartsoort",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/minio_to_postgres/bezetting_per_kwartier_kaartsoort.py",
            spark_executor_cores=2,
        ),
    ]

    end_to_postgres = DummyOperator(task_id="end_to_postgres", dag=dag)

    for to_postgres_job in to_postgres_jobs:
        add_job_to_node(start, to_postgres_job, timestamp_str, end_to_postgres)
