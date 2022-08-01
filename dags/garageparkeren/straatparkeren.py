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
    schedule_interval=INTERVAL,
    catchup=False,
    max_active_runs=1,
) as dag:

    start = DummyOperator(
        task_id="start", dag=dag
    )
    end = DummyOperator(task_id="end", dag=dag)

    run_straatparkeren_eps_nha1 = TriggerDagRunOperator(
        trigger_dag_id="straatparkeren-eps-nha1",
        task_id="run-straatparkeren-eps-nha1",
        dag=dag,
    )

    check_straatparkeren_eps_nha1 = DagSensor(
        dag=dag,
        task_id="watch-straatparkeren-eps-nha1",
        external_dag_id="straatparkeren-eps-nha1",
    )

    run_straatparkeren_eps_scan1 = TriggerDagRunOperator(
        trigger_dag_id="straatparkeren-eps-scan1",
        task_id="run-straatparkeren-eps-scan1",
        dag=dag,
    )

    check_straatparkeren_eps_scan1 = DagSensor(
        dag=dag,
        task_id="watch-straatparkeren-eps-scan1",
        external_dag_id="straatparkeren-eps-scan1",
    )

    run_straatparkeren_rdw1 = TriggerDagRunOperator(
        trigger_dag_id="straatparkeren-rdw1",
        task_id="run-scn1-staging-to-datamart",
        dag=dag,
    )

    check_straatparkeren_rdw1 = DagSensor(
        dag=dag,
        task_id="watch-straatparkeren-rdw1",
        external_dag_id="straatparkeren-rdw1",
    )

    (
            start
            >> run_straatparkeren_eps_nha1
            >> check_straatparkeren_eps_nha1
            >> end
    )
    (
            start
            >> run_straatparkeren_eps_scan1
            >> check_straatparkeren_eps_scan1
            >> end
    )
    (
            start
            >> run_straatparkeren_rdw1
            >> check_straatparkeren_rdw1
            >> end
    )
