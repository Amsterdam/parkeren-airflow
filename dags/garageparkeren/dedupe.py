from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from operators.kubernetes import JobOperator, JobSensor, BaseOperator
from dags.garageparkeren.common import (
    generate_job,
    NAMESPACE,
    MAX_JOB_NAME_LENGTH,
    IMAGE,
    job_sensor_poke_jitter,
)

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

DAG_ID = "garageparkerenraw-dedupe-historic"

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

    source_systems = ["snb1", "ipp1", "ski2", "ski3", "scn1"]

    for source_system in source_systems:
        historic_dedupe = generate_job(
            f"{source_system}-historic-dedupe-{timestamp_str}"[
                :MAX_JOB_NAME_LENGTH
            ].rstrip("-"),
            NAMESPACE,
            IMAGE,
            "/app/src/jobs/staging_to_historic/historic_dedupe.py",
            arguments=[source_system],
        )

        run_historic_dedupe = JobOperator(
            job=historic_dedupe, task_id=f"run-{source_system}-historic-dedupe"
        )

        watch_historic_dedupe: BaseOperator = JobSensor(
            job_name=historic_dedupe.metadata.name,
            task_id=f"watch-{source_system}-historic-dedupe",
            namespace=NAMESPACE,
            poke_interval=60 + job_sensor_poke_jitter(),
        )

        dedupe_copy = generate_job(
            f"{source_system}-dedupe-copy-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip(
                "-"
            ),
            NAMESPACE,
            IMAGE,
            "/app/src/jobs/staging_to_historic/historic_dedupe_copy.py",
            arguments=[source_system],
        )

        run_dedupe_copy = JobOperator(
            job=dedupe_copy, task_id=f"run-{source_system}-dedupe-copy"
        )

        watch_dedupe_copy: BaseOperator = JobSensor(
            job_name=dedupe_copy.metadata.name,
            task_id=f"watch-{source_system}-dedupe-copy",
            namespace=NAMESPACE,
            poke_interval=60 + job_sensor_poke_jitter(),
        )

        (
            start
            >> run_historic_dedupe
            >> watch_historic_dedupe
            >> run_dedupe_copy
            >> watch_dedupe_copy
        )
