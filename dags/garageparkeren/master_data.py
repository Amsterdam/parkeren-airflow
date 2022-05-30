from datetime import datetime, timedelta
from common import MessageOperator
from airflow import DAG
from airflow.operators.dummy import DummyOperator

from operators.kubernetes import JobOperator, JobSensor, BaseOperator
from dags.garageparkeren.common import (
    generate_job,
    NAMESPACE,
    MAX_JOB_NAME_LENGTH,
    IMAGE,
)

ARGS = {
    "owner": "garageparkeren - thomask",
    "description": "",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

DAG_ID = "garageparkeren-master-data"

INTERVAL = None

with DAG(
    DAG_ID,
    schedule_interval=INTERVAL,
    default_args=ARGS,
    catchup=False,
    max_active_runs=1,
) as dag:
    start = datetime.now()
    timestamp_str = start.strftime("%Y%m%d")

    start = DummyOperator(task_id="start", dag=dag)

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    master_data_job = generate_job(
        job_name=f"master-data-spark-job-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip(
            "-"
        ),
        namespace=NAMESPACE,
        image=IMAGE,
        job_script_path="/app/src/jobs/master_data/run_all_master.py",
        spark_driver_cores=1,
        spark_driver_memory_gb=1,
        spark_executor_cores=2,
        spark_executor_memory_gb=2,
        spark_executor_instances=2,
    )

    run_master_data_job = JobOperator(job=master_data_job, task_id=f"run-master-data-spark-job-thomas")

    watch_master_data_job: BaseOperator = JobSensor(
        job_name=master_data_job.metadata.name,
        task_id=f"watch-master-data-spark-job",
        namespace=NAMESPACE,
    )
    slack_at_start >> start >> run_master_data_job >> watch_master_data_job
