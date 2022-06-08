from datetime import datetime, timedelta
from operators.dag_sensor import DagSensor

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator

DAG_ID = "garageparkerenraw-all"
INTERVAL = "1 16 * * *"

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

with DAG(
    DAG_ID,
    default_args=ARGS,
    schedule_interval=INTERVAL,
    catchup=False,
    max_active_runs=1,
) as dag:

    # start_validation = DummyOperator(task_id="start_validation", dag=dag)
    #
    # run_validate = TriggerDagRunOperator(
    #     trigger_dag_id="garageparkerenraw-validate", task_id="run-validate", dag=dag
    # )
    #
    # check_validate = DagSensor(
    #     dag=dag, task_id="watch-validate", external_dag_id="garageparkerenraw-validate"
    # )

    start_staging_to_datamart = DummyOperator(
        task_id="start_staging_to_datamart", dag=dag
    )

    end_staging_to_datamart = DummyOperator(task_id="end_staging_to_datamart", dag=dag)

    end = DummyOperator(task_id="end", dag=dag)

    run_ipp1_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkerenraw-ipp1",
        task_id="run-ipp1-staging-to-datamart",
        dag=dag,
    )

    run_scn1_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkerenraw-scn1",
        task_id="run-scn1-staging-to-datamart",
        dag=dag,
    )

    run_snb1_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkerenraw-snb1",
        task_id="run-snb1-staging-to-datamart",
        dag=dag,
    )

    run_ski2_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkerenraw-ski2",
        task_id="run-ski2-staging-to-datamart",
        dag=dag,
    )

    run_ski3_staging_to_datamart = TriggerDagRunOperator(
        trigger_dag_id="garageparkerenraw-ski3",
        task_id="run-ski3-staging-to-datamart",
        dag=dag,
    )

    check_ipp1_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-ipp1-staging-to-datamart",
        external_dag_id="garageparkerenraw-ipp1",
    )

    check_scn1_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-scn1staging-to-datamart",
        external_dag_id="garageparkerenraw-scn1",
    )

    check_snb1_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-snb1-staging-to-datamart",
        external_dag_id="garageparkerenraw-snb1",
    )

    check_ski2_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-ski2-staging-to-datamart",
        external_dag_id="garageparkerenraw-ski2",
    )

    check_ski3_staging_to_datamart = DagSensor(
        dag=dag,
        task_id="watch-ski3-staging-to-datamart",
        external_dag_id="garageparkerenraw-ski3",
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

    # run_to_postgres = TriggerDagRunOperator(
    #     trigger_dag_id="garageparkerenraw-to-postgres",
    #     task_id="run-to-postgres",
    #     dag=dag,
    #     wait_for_completion=True,
    # )
    #
    # check_to_postgres = DagSensor(
    #     dag=dag,
    #     task_id="watch-to-postgres",
    #     external_dag_id="garageparkerenraw-to-postgres",
    # )

    # run_dedupe_historic = TriggerDagRunOperator(
    #     trigger_dag_id="garageparkerenraw-dedupe-historic",
    #     task_id="run-dedupe-historic",
    #     dag=dag,
    #     wait_for_completion=True,
    # )
    #
    # check_dedupe_historic = DagSensor(
    #     dag=dag,
    #     task_id="watch-dedupe-historic",
    #     external_dag_id="garageparkerenraw-dedupe-historic",
    # )

    # run_rdw1_staging_to_historic = TriggerDagRunOperator(
    #     trigger_dag_id="garageparkerenraw-rdw1",
    #     task_id="run-rdw1-staging-to-historic",
    #     dag=dag,
    #     wait_for_completion=True,
    # )
    #
    # check_rdw1_staging_to_historic = DagSensor(
    #     dag=dag,
    #     task_id="watch-rdw1-staging-to-historic",
    #     external_dag_id="garageparkerenraw-rdw1",
    # )
    #
    # run_eps1_staging_to_historic = TriggerDagRunOperator(
    #     trigger_dag_id="garageparkerenraw-eps1",
    #     task_id="run-eps1-staging-to-historic",
    #     dag=dag,
    #     wait_for_completion=True,
    # )
    #
    # check_eps1_staging_to_historic = DagSensor(
    #     dag=dag,
    #     task_id="watch-eps1-staging-to-historic",
    #     external_dag_id="garageparkerenraw-eps1",
    # )

    end_staging_to_datamart >> end
    # end_staging_to_datamart >> run_rdw1_staging_to_historic >> check_rdw1_staging_to_historic >> end
    # end_staging_to_datamart >> run_eps1_staging_to_historic >> check_eps1_staging_to_historic >> end

    (
        end_staging_to_datamart
        >> run_scn1_staging_to_datamart
        >> check_scn1_staging_to_datamart
        # >> run_dedupe_historic
        # >> check_dedupe_historic
        >> end
    )
