from datetime import datetime, timedelta
from common import MessageOperator
from airflow import DAG
from airflow.operators.dummy import DummyOperator

from operators.kubernetes import JobOperator, JobSensor, BaseOperator
from dags.garageparkeren.common import (
    generate_job,
    NAMESPACE,
    MAX_JOB_NAME_LENGTH,
    OWNER,
    IMAGE
)


ARGS = {
    "owner": OWNER,
    "description": "",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

DAG_ID = "garageparkeren-test-bas"

INTERVAL = None
# INTERVAL = timedelta(hours=1)

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

    for job in range(1):
        test_job = generate_job(
            job_name=f"bas-spark-job-{job}-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip(
                "-"
            ),
            namespace=NAMESPACE,
            image="parkerenweuacrow77kin67.azurecr.io/parkeren-spark:bas",
            job_script_path="/app/src/jobs/......",
            spark_driver_cores=1,
            spark_driver_memory_gb=1,
            spark_executor_cores=2,
            spark_executor_memory_gb=2,
            spark_executor_instances=1,
            # arguments=["argument"]
        )

        run_test_job = JobOperator(job=test_job, task_id=f"run-test-spark-job-bas-{job}")

        watch_test_job: BaseOperator = JobSensor(
            job_name=test_job.metadata.name,
            task_id=f"watch-test-spark-job-{job}",
            namespace=NAMESPACE,
        )
        start >> run_test_job >> watch_test_job
