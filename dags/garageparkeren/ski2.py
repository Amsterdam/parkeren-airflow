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

DAG_ID = "garageparkeren-ski2"

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
    timestamp_str = start.strftime("%Y%m%d%H")

    start = DummyOperator(task_id="start", dag=dag)

    # To historic
    # staging_to_his = generate_job(
    #     f"ski2-sta-to-his-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip("-"),
    #     NAMESPACE,
    #     IMAGE,
    #     "/app/src/jobs/staging_to_historic/ski2/job.py",
    #     spark_driver_cores=2,
    #     spark_driver_memory_gb=8,
    #     spark_executor_cores=4,
    #     spark_executor_memory_gb=8,
    #     spark_executor_instances=2,
    # )
    #
    # run_staging_to_his = JobOperator(job=staging_to_his, task_id="run-ski2-sta-to-his")
    #
    # watch_staging_to_his: BaseOperator = JobSensor(
    #     job_name=staging_to_his.metadata.name,
    #     task_id="watch-ski2-sta-to-his",
    #     namespace=NAMESPACE,
    #     poke_interval=60 + job_sensor_poke_jitter(),
    # )
    #
    # start >> run_staging_to_his >> watch_staging_to_his

    # staging_to_archive = generate_job(
    #     job_name=f"ski2-sta-to-arch-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip("-"),
    #     namespace=NAMESPACE,
    #     image=IMAGE,
    #     job_script_path="/app/src/jobs/staging_to_historic/ski2/archive.py",
    #     spark_driver_cores=1,
    #     spark_driver_memory_gb=8,
    #     spark_executor_cores=2,
    #     spark_executor_memory_gb=8,
    #     spark_executor_instances=3,
    # )
    #
    # run_staging_to_archive = JobOperator(
    #     job=staging_to_archive, task_id="run-ski2-sta-to-arch"
    # )
    #
    # watch_staging_to_archive: BaseOperator = JobSensor(
    #     job_name=staging_to_archive.metadata.name,
    #     task_id="watch-ski2-sta-to-arch",
    #     namespace=NAMESPACE,
    #     poke_interval=60 + job_sensor_poke_jitter(),
    # )

    # watch_staging_to_his >> run_staging_to_archive >> watch_staging_to_archive

    # 6 cores 41 mem
    to_integration_jobs = [
        SparkJob(
            job="ski2-his-to-int-betaalregel",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/historic_to_integration/ski2/betaalregel.py",
            spark_executor_cores=1,
        ),
        SparkJob(
            job="ski2-his-to-int-parkeerbeweging",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/historic_to_integration/ski2/parkeerbeweging.py",
            spark_executor_cores=1,
        ),
        SparkJob(
            job="ski2-his-to-int-telling",
            spark_driver_memory_gb=1,
            spark_executor_memory_gb=1,
            spark_executor_instances=1,
            python_path="/app/src/jobs/historic_to_integration/ski2/telling.py",
            spark_executor_cores=1,
        ),
        SparkJob(
            job="ski2-his-to-int-transactieafrekening",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=1,
            python_path="/app/src/jobs/historic_to_integration/ski2/transactieafrekening.py",
            spark_executor_cores=1,
        ),
    ]

    end_to_int = DummyOperator(task_id="end_to_int", dag=dag)

    for to_integration_job in to_integration_jobs:
        add_job_to_node(
            start, to_integration_job, timestamp_str, end_to_int
        )

    # 8 cores 48 mem
    to_datamart_jobs = [
        SparkJob(
            job="ski2-int-to-datamart-bezetting",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/integration_to_datamart/bezetting.py",
            spark_executor_cores=2,
            arguments=["ski2"],
        ),
        SparkJob(
            job="ski2-int-to-datamart-opbrengst",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/integration_to_datamart/opbrengst.py",
            spark_executor_cores=1,
            arguments=["ski2"],
        ),
        SparkJob(
            job="ski2-int-to-datamart-parkeerduur",
            spark_driver_memory_gb=4,
            spark_executor_memory_gb=8,
            spark_executor_instances=2,
            python_path="/app/src/jobs/integration_to_datamart/parkeerduur.py",
            spark_executor_cores=1,
            arguments=["ski2"],
        ),
    ]

    end_to_datamart = DummyOperator(task_id="end_to_datamart", dag=dag)

    for to_datamart_job in to_datamart_jobs:
        add_job_to_node(end_to_int, to_datamart_job, timestamp_str, end_to_datamart)
