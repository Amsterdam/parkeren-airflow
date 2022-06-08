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

DAG_ID = "garageparkerenraw-validate"

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

    source_systems = ["ipp1", "ski2", "ski3", "snb1", "scn1"]

    end_validate_staging_tables_exist = DummyOperator(
        task_id="end_validate_staging_tables_exist", dag=dag
    )
    end_collect_garage_cdes = DummyOperator(task_id="end_collect_garage_cdes", dag=dag)

    for source_system in source_systems:
        validate_staging_tables_exist = SparkJob(
            job=f"{source_system}-validate-staging-tables-exist",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=2,
            spark_executor_instances=1,
            python_path="/app/src/jobs/data_quality/validate_staging_tables_exist.py",
            spark_executor_cores=1,
            arguments=[source_system],
        )
        add_job_to_node(
            start,
            validate_staging_tables_exist,
            timestamp_str,
            end_validate_staging_tables_exist,
        )
        collect_garage_cdes = SparkJob(
            job=f"{source_system}-collect-garage-cdes",
            spark_driver_memory_gb=2,
            spark_executor_memory_gb=2,
            spark_executor_instances=1,
            python_path="/app/src/jobs/data_quality/collect_garage_cdes.py",
            spark_executor_cores=1,
            arguments=[source_system],
        )
        add_job_to_node(
            end_validate_staging_tables_exist,
            collect_garage_cdes,
            timestamp_str,
            end_collect_garage_cdes,
        )

    validate_garage_cdes = SparkJob(
        job="validate-garage-cdes",
        spark_driver_memory_gb=2,
        spark_executor_memory_gb=2,
        spark_executor_instances=1,
        python_path="/app/src/jobs/data_quality/validate_garage_cdes.py",
        spark_executor_cores=1,
    )

    end_validate_garage_cdes = DummyOperator(
        task_id="end_validate_garage_cdes", dag=dag
    )

    add_job_to_node(
        end_collect_garage_cdes,
        validate_garage_cdes,
        timestamp_str,
        end_validate_garage_cdes,
    )
