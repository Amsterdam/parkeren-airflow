from datetime import datetime, timedelta
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

DAG_ID = "garageparkeren-migrate"

INTERVAL = None

source_systems_jobs_stage_one = [
    "snb1",
    "ipp1",
    "ipp2",
    # "ski1",
    # "ski2",
    # "ski3",
    # "scn1",
]

source_systems_jobs_stage_two = [
    # "snb1"
    # "ipp1",
    # "ipp2",
    # "ski1",
    # "ski2",
    "ski3",
    "scn1",
]

arguments = {
    "ipp1": [
        "CashierAction",
        "CityPassTransaction",
        "ExcessiveParkingTime",
        "Invoice",
        "Occupation",
        "ParkingTransaction",
        "Payment",
        "Reduction",
        "SubscriptionModelOccupation",
        "SubscriptionModelOccupationCorrection",
        "TerminalLog",
        "TerminalRequestException",
        "vwCounterCorrection",
        "vwCounterSample",
    ],
    "ipp2": [
        "CityPassTransaction",
        "Company",
        "CountBlock",
        "CounterCorrection",
        "CounterSample",
        "DaySchedule",
        "ExcessiveParkingTime",
        "FacilityFee",
        "Facility",
        "FeeElement",
        "FeeFolder",
        "FeeSchedule",
        "GateMovement",
        "Identification",
        "Invoice",
        "ManualCorrection",
        "ManualPassRequest",
        "ParkingTransaction",
        "Payment",
        "PoolUsage",
        "ReductionCardProfile",
        "Reduction",
        "ReservationFacility",
        "Reservation",
        "SiteAccess",
        "Subscriber",
        "SubscriptionModelSiteAccess",
        "SubscriptionModel",
        "SubSectionAccess",
        "SubSectionCounters",
        "SubSection",
        "TerminalLog",
        "TerminalRequestException",
        "Terminal",
        "TimeDiagramRange",
        "TimeDiagram",
        "TimeSchedule",
        "TvsReductionProfile",
    ],
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
    "ski3": [
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
    "snb1": [
        "ADDR_G",
        "AUSLAST",
        "GEOMETRY_L",
        "INVTRANSDATA",
        "LOGGCARD",
        "LOGGVORGANG",
        "PTCPTADMIN_G",
        "PTCPT_G",
        "RECHNUNG",
        "TMOVEMENT",
        "TSHIFT",
    ]
}

def generate_stage(source_system: str, start: BaseOperator, end: BaseOperator, timestamp_str: str):
    migrate_job = generate_job(
        job_name=f"migrate-spark-job-{source_system}-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip(
            "-"
        ),
        namespace=NAMESPACE,
        image=IMAGE,
        job_script_path=f"/app/src/jobs/migration/{source_system}/migrate.py",
        spark_driver_cores=1,
        spark_driver_memory_gb=8,
        spark_executor_cores=2,
        spark_executor_memory_gb=8,
        spark_executor_instances=2,
        arguments=arguments[source_system]
    )

    run_migrate_job = JobOperator(job=migrate_job, task_id=f"run-migrate-spark-job-{source_system}")

    watch_migrate_job: BaseOperator = JobSensor(
        job_name=migrate_job.metadata.name,
        task_id=f"watch-migrate-spark-job-{source_system}",
        namespace=NAMESPACE,
    )

    staging_job = generate_job(
        job_name=f"sta-to-his-spark-job-{source_system}-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip(
            "-"
        ),
        namespace=NAMESPACE,
        image=IMAGE,
        job_script_path=f"/app/src/jobs/staging_to_historic/{source_system}/job.py",
        spark_driver_cores=1,
        spark_driver_memory_gb=8,
        spark_executor_cores=2,
        spark_executor_memory_gb=8,
        spark_executor_instances=2,
        arguments=arguments[source_system]
    )

    run_staging_job = JobOperator(job=staging_job, task_id=f"run-sta-to-his-spark-job-{source_system}")

    watch_staging_job: BaseOperator = JobSensor(
        job_name=staging_job.metadata.name,
        task_id=f"watch-sta-to-his-spark-job-{source_system}",
        namespace=NAMESPACE,
    )

    start >> run_migrate_job >> watch_migrate_job >> run_staging_job >> watch_staging_job >> end


with DAG(
    DAG_ID,
    schedule_interval=INTERVAL,
    default_args=ARGS,
    catchup=False,
    max_active_runs=1,
) as dag:

    start_time = datetime.now()
    timestamp_str = start_time.strftime("%Y%m%d")
    start: BaseOperator = DummyOperator(task_id="start", dag=dag)
    end: BaseOperator = DummyOperator(task_id="end", dag=dag)

    for source_system_job_stage_one in source_systems_jobs_stage_one:
        generate_stage(source_system_job_stage_one, start, end, timestamp_str)
