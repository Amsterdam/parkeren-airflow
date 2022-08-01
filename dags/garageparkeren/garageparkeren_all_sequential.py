from datetime import datetime, timedelta
from operators.dag_sensor import DagSensor

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from dags.garageparkeren.common import OWNER, RUN_ALL_INTERVAL

DAG_ID = "garageparkeren-all-sequential"

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
INTERVAL = None

with DAG(
    DAG_ID,
    default_args=ARGS,
    schedule_interval="1 4 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    start_stage_one = DummyOperator(
        task_id="start_stage_one", dag=dag
    )
    start_stage_two = DummyOperator(task_id="start_stage_two", dag=dag)
    start_to_csv = DummyOperator(task_id="start_to_csv", dag=dag)

    run_ipp1_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-ipp1",
        task_id="run-ipp1-staging-to-datamart",
        dag=dag,
    )

    run_ipp2_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkeren-ipp2",
        task_id="run-ipp2-staging-to-datamart",
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

    check_ipp2_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-ipp2-staging-to-datamart",
        external_dag_id="garageparkeren-ipp2",
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
            start_stage_one
            >> run_ipp1_staging_to_datamart
            >> check_ipp1_staging_to_datamart
            >> start_stage_two
    )
    (
            start_stage_one
            >> run_ipp2_staging_to_datamart
            >> check_ipp2_staging_to_datamart
            >> start_stage_two
    )
    (
            start_stage_one
            >> run_snb1_staging_to_datamart
            >> check_snb1_staging_to_datamart
            >> start_stage_two
    )
    (
            start_stage_two
            >> run_ski2_staging_to_datamart
            >> check_ski2_staging_to_datamart
            >> start_to_csv
    )
    (
            start_stage_two
            >> run_ski3_staging_to_datamart
            >> check_ski3_staging_to_datamart
            >> start_to_csv
    )
    (
            start_stage_two
            >> run_scn1_staging_to_datamart
            >> check_scn1_staging_to_datamart
            >> start_to_csv
    )
    start_to_csv >> run_to_csv >> check_to_csv
