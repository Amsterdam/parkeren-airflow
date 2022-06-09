from datetime import datetime, timedelta
from operators.dag_sensor import DagSensor

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from dags.garageparkeren.common import OWNER

DAG_ID = "garageparkeren-all"
INTERVAL = "1 16 * * *"

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

with DAG(
    DAG_ID,
    default_args=ARGS,
    schedule_interval=INTERVAL,
    catchup=False,
    max_active_runs=1,
) as dag:

    start_staging_to_datamart = DummyOperator(
        task_id="start_staging_to_datamart", dag=dag
    )

    end_staging_to_datamart = DummyOperator(task_id="end_staging_to_datamart", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    run_ipp1_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-ipp1",
        task_id="run-ipp1-staging-to-datamart",
        dag=dag,
    )

    run_scn1_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-scn1",
        task_id="run-scn1-staging-to-datamart",
        dag=dag,
    )

    run_snb1_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-snb1",
        task_id="run-snb1-staging-to-datamart",
        dag=dag,
    )

    run_ski2_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-ski2",
        task_id="run-ski2-staging-to-datamart",
        dag=dag,
    )

    run_ski3_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-ski3",
        task_id="run-ski3-staging-to-datamart",
        dag=dag,
    )

    check_ipp1_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-ipp1-staging-to-datamart",
        external_dag_id="garageparkeren-ipp1",
    )

    check_scn1_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-scn1staging-to-datamart",
        external_dag_id="garageparkeren-scn1",
    )

    check_snb1_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-snb1-staging-to-datamart",
        external_dag_id="garageparkeren-snb1",
    )

    check_ski2_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-ski2-staging-to-datamart",
        external_dag_id="garageparkeren-ski2",
    )

    check_ski3_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-ski3-staging-to-datamart",
        external_dag_id="garageparkeren-ski3",
    )

    run_to_csv = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-to-csv",
        task_id="run-garageparkeren-to-csv",
        dag=dag,
    )

    check_to_csv = DagSensor(
        dag=dag,
        task_id="watch-garageparkeren-to-csv",
        external_dag_id="garageparkeren-to-csv",
    )

    (
        start_staging_to_datamart
        >> run_ipp1_staging_to_datamart
        >> check_ipp1_staging_to_datamart
        >> end_staging_to_datamart
    )
    (
        start_staging_to_datamart
        >> run_snb1_staging_to_datamart
        >> check_snb1_staging_to_datamart
        >> end_staging_to_datamart
    )
    (
        start_staging_to_datamart
        >> run_ski2_staging_to_datamart
        >> check_ski2_staging_to_datamart
        >> end_staging_to_datamart
    )
    (
        start_staging_to_datamart
        >> run_ski3_staging_to_datamart
        >> check_ski3_staging_to_datamart
        >> end_staging_to_datamart
    )
    (
        start_staging_to_datamart
        >> run_scn1_staging_to_datamart
        >> check_scn1_staging_to_datamart
        >> end_staging_to_datamart
    )
    end_staging_to_datamart >> run_to_csv >> check_to_csv
