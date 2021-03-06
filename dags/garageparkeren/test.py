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
    OWNER
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

DAG_ID = "garageparkeren-test"

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

    # SparkJob(
    #     job="ski3-int-to-datamart-opbrengst",
    #     spark_driver_memory_gb=2,
    #     spark_executor_memory_gb=8,
    #     spark_executor_instances=2,
    #     python_path="/app/src/jobs/integration_to_datamart/opbrengst.py",
    #     spark_executor_cores=1,
    #     source_system="ski3",
    # ),

    # slack_at_start = MessageOperator(
    #     task_id="slack_at_start",
    # )

    for job in range(1):
        test_job = generate_job(
            job_name=f"ipp1-sta-to-his-spark-job-{job}-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip(
                "-"
            ),
            namespace=NAMESPACE,
            image=IMAGE,
            job_script_path="/app/src/jobs/staging_to_historic/ipp1/job.py",
            spark_driver_cores=1,
            spark_driver_memory_gb=8,
            spark_executor_cores=2,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
        )

        run_test_job = JobOperator(job=test_job, task_id=f"run-ipp1-sta-to-his-spark-job-thomas-{job}")

        watch_test_job: BaseOperator = JobSensor(
            job_name=test_job.metadata.name,
            task_id=f"watch-ipp1-sta-to-his-spark-job-{job}",
            namespace=NAMESPACE,
        )

        start >> run_test_job >> watch_test_job
