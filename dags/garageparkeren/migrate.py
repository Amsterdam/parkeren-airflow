from datetime import datetime, timedelta
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
    "owner": "garageparkeren",
    "description": "",
    "depend_on_past": False,
    "start_date": datetime(2020, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}

DAG_ID = "garageparkeren-migrate"

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

    source_systems_jobs = [
        "ipp1",
        "scn1",
        "ski1",
        "ski2",
        "ski3",
        "snb1"
    ]

    arguments = {
        "ipp1": [],
        "scn1": [
            "Abonnementen",
            "Betaling",
            "Dienstafrekening",
            "Dienstafrekeningomzet",
            "Omzet",
            "Omzet_per_product",
            "Parkeerapparaten",
            "Parkeerbewegingen",
            "Parkeerkaarten",
            "Tellingen"
        ],
        "ski1": [
            "Articles",
            "Automatic_Payment_Machine_Levels",
            "Car_Parks",
            "Cash_flow",
            "Cash_transactions",
            "Cheque_transactions",
            "Credit_card_transactions",
            "Customer",
            "Electronic_purse_transactions",
            "Entries",
            "Invoice_transactions",
            "Occupancy_Statistics",
            "Other_payment_transactions",
            "Parking_payment_transactions",
            "Parking_transactions",
            "Parking_transactions_contract_parker_cards",
            "Pay_stations_levels",
            "Sale_payment_transactions",
            "Shift_reports_of_automatic_payment_machines",
            "Shift_reports_of_pay_stations",
            "System_Devices",
            "Token_transactions",
            "Validation_transactions",
            "Value_card_transactions"
        ],
        "ski2": [
            "Articles",
            "Automatic_payment_machine_levels",
            "Automatic_payment_machine_transactions",
            "Car_park_counting",
            "Car_parks",
            "Card",
            "Cash_flow",
            "Cash_transactions",
            "Cheque_transactions",
            "Counting_category",
            "Credit_card_transactions",
            "Customer",
            "Customer_car_park_counting",
            "Customer_counting",
            "Electronic_purse_transactions",
            "Entries",
            "Facilities",
            "Invoice_transactions",
            "Manual_payments",
            "Max_levels_per_car_park_for_group_counting",
            "Occupancy_statistics",
            "Occupancy_statistics_details",
            "Other_payment_transactions",
            "Parking_payment_transactions",
            "Parking_transactions",
            "Parking_transactions_contract_parker_cards",
            "Partial_rates",
            "Passage_statistics",
            "Passage_statistics_details",
            "Pay_station_levels",
            "Personal",
            "Rates",
            "Sales_payment_transactions",
            "Shift_reports_of_automatic_payment_machines",
            "Shift_reports_of_pay_stations",
            "Storey_counting",
            "System_devices",
            "System_events",
            "Token_transactions",
            "Transactions_card_system_cards",
            "Transactions_contract_parkers",
            "Transactions_credit_card",
            "Transactions_parking_card",
            "User",
            "Valid_car_parks",
            "Validation_providers",
            "Validation_transactions",
            "Value_added_tax",
            "Value_card_transactions"
        ],
        "ski3": [],
        "snb1": []
    }

    for source_system_job in source_systems_jobs:
        test_job = generate_job(
            job_name=f"migrate-spark-job-{source_system_job}-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip(
                "-"
            ),
            namespace=NAMESPACE,
            image=IMAGE,
            # job_script_path=f"/app/src/jobs/migration/{source_system_job}/migrate.py",
            job_script_path="/app/src/spark_thomas.py",
            spark_driver_cores=1,
            spark_driver_memory_gb=1,
            spark_executor_cores=2,
            spark_executor_memory_gb=2,
            spark_executor_instances=2,
            arguments=arguments[source_system_job]
        )

        run_test_job = JobOperator(job=test_job, task_id=f"run-migrate-spark-job123-{source_system_job}")

        watch_test_job: BaseOperator = JobSensor(
            job_name=test_job.metadata.name,
            task_id=f"watch-migrate-spark-job-{source_system_job}",
            namespace=NAMESPACE,
        )
        start >> run_test_job >> watch_test_job
