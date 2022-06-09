from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from operators.kubernetes import JobOperator, JobSensor, BaseOperator
from dags.garageparkeren.common import (
    generate_job,
    NAMESPACE,
    MAX_JOB_NAME_LENGTH,
    IMAGE,
    SparkJob,
    add_job_to_node,
    job_sensor_poke_jitter,
    OWNER
)

ARGS = {
    "owner": OWNER,
    "description": "",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=15),
}

DAG_ID = "garageparkeren-scn1"

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

    # To historic
    staging_to_his = generate_job(
        f"scn1-sta-to-his-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip("-"),
        NAMESPACE,
        IMAGE,
        "/app/src/jobs/staging_to_historic/scn1/job.py",
    )

    run_staging_to_his = JobOperator(job=staging_to_his, task_id="run-scn1-sta-to-his")

    watch_staging_to_his: BaseOperator = JobSensor(
        job_name=staging_to_his.metadata.name,
        task_id="watch-scn1-sta-to-his",
        namespace=NAMESPACE,
        poke_interval=60 + job_sensor_poke_jitter(),
    )

    start >> run_staging_to_his >> watch_staging_to_his

    to_integration_jobs = [
        SparkJob(
            job="scn1-his-to-int-opbrengsten",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/historic_to_integration/scn1/opbrengsten.py",
            spark_executor_cores=1,
        ),
        SparkJob(
            job="scn1-his-to-int-transacties",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/historic_to_integration/scn1/transacties.py",
            spark_executor_cores=1,
        ),
    ]

    end_to_int = DummyOperator(task_id="end_to_int", dag=dag)

    for to_integration_job in to_integration_jobs:
        add_job_to_node(
            watch_staging_to_his, to_integration_job, timestamp_str, end_to_int
        )
