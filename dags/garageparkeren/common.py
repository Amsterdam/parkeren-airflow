from dataclasses import dataclass
from random import uniform

from kubernetes import client
from operators.kubernetes import JobOperator, JobSensor, BaseOperator

KUBERNETES_MEMORY_OVERHEAD_FACTOR = 0.1
NAMESPACE = "airflow-mobibbn1"
IMAGE = "parkerenweuacrow77kin67.azurecr.io/parkeren-spark:thomas"
MAX_JOB_NAME_LENGTH = 63


def generate_job(
    job_name: str,
    namespace: str,
    image: str,
    job_script_path: str,
    spark_driver_cores: int = 1,
    spark_driver_memory_gb: int = 1,
    spark_executor_cores: int = 1,
    spark_executor_memory_gb: int = 1,
    spark_executor_instances: int = 1,
    #     TODO change naming
    source_system: str = "",
) -> client.V1Job:
    kubernetes_memory_mb = int(
        ((1 + KUBERNETES_MEMORY_OVERHEAD_FACTOR) * 1000) * spark_driver_memory_gb
    )
    return client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, namespace=namespace, labels={"job_type": "spark"}),
        spec=client.V1JobSpec(
            backoff_limit=3,
            active_deadline_seconds=57600,
            ttl_seconds_after_finished=60,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    service_account_name="spark",
                    node_selector={"nodetype": "mobibbn1work"},
                    # volumes=[
                    #     client.V1Volume(
                    #         name="spark-defaults",
                    #         config_map=client.V1ConfigMapVolumeSource(
                    #             default_mode=420,
                    #             name="spark-defaults",
                    #             items=[
                    #                 client.V1KeyToPath(
                    #                     key="spark-defaults", path="spark-defaults.conf"
                    #                 )
                    #             ],
                    #         ),
                    #     )
                    # ],
                    containers=[
                        client.V1Container(
                            name="driver",
                            image=image,
                            image_pull_policy="Always",
                            command=["python3", job_script_path, source_system],
                            env=[
                                client.V1EnvVar(
                                    name="POD_NAME",
                                    value_from=client.V1EnvVarSource(field_ref=client
                                                                     .V1ObjectFieldSelector(field_path="metadata.name"))
                                ),
                                client.V1EnvVar(
                                    name="SPARK_DRIVER_BIND_ADDRESS",
                                    value_from=client.V1EnvVarSource(field_ref=client
                                                                     .V1ObjectFieldSelector(field_path="status.podIP"))
                                ),
                                client.V1EnvVar(
                                    name="SPARK_LOCAL_IP",
                                    value_from=client.V1EnvVarSource(field_ref=client
                                                                     .V1ObjectFieldSelector(field_path="status.podIP"))
                                ),
                                client.V1EnvVar(
                                    name="IMAGE",
                                    value=str(image),
                                ),
                                client.V1EnvVar(
                                    name="NAMESPACE",
                                    value=str(namespace),
                                ),
                                client.V1EnvVar(
                                    name="SPARK_DRIVER_CORES",
                                    value=str(spark_driver_cores),
                                ),
                                client.V1EnvVar(
                                    name="SPARK_DRIVER_MEMORY",
                                    value=f"{spark_driver_memory_gb}g",
                                ),
                                client.V1EnvVar(
                                    name="SPARK_EXECUTOR_CORES",
                                    value=str(spark_executor_cores),
                                ),
                                client.V1EnvVar(
                                    name="SPARK_EXECUTOR_MEMORY",
                                    value=f"{spark_executor_memory_gb}g",
                                ),
                                client.V1EnvVar(
                                    name="SPARK_EXECUTOR_INSTANCES",
                                    value=str(spark_executor_instances),
                                ),
                                client.V1EnvVar(
                                    name="PYSPARK_DRIVER_PYTHON",
                                    value="/usr/bin/python3",
                                ),
                                client.V1EnvVar(
                                    name="PYSPARK_PYTHON", value="/usr/bin/python3"
                                ),
                            ],
                            resources=client.V1ResourceRequirements(
                                requests=dict(
                                    cpu=spark_driver_cores,
                                    memory=f"{kubernetes_memory_mb}M",
                                ),
                                limits=dict(
                                    cpu=spark_driver_cores,
                                    memory=f"{kubernetes_memory_mb}M",
                                ),
                            ),
                            # volume_mounts=[
                            #     client.V1VolumeMount(
                            #         name="spark-defaults",
                            #         sub_path="spark-defaults.conf",
                            #         mount_path="/opt/spark/conf/spark-defaults.conf",
                            #     )
                            # ],
                        )
                    ],
                )
            ),
        ),
    )


@dataclass
class SparkJob:

    job: str
    spark_driver_memory_gb: int
    spark_executor_memory_gb: int
    spark_executor_instances: int
    python_path: str
    spark_driver_cores: int = 1
    spark_executor_cores: int = 1
    source_system: str = ""


def add_job_to_node(
    start_node: BaseOperator,
    spark_job: SparkJob,
    timestamp_str: str,
    end_node: BaseOperator,
):
    job_name = spark_job.job
    his_to_int = generate_job(
        f"{job_name}-{timestamp_str}"[:MAX_JOB_NAME_LENGTH].rstrip("-"),
        NAMESPACE,
        IMAGE,
        spark_job.python_path,
        spark_driver_memory_gb=spark_job.spark_driver_memory_gb,
        spark_executor_memory_gb=spark_job.spark_executor_memory_gb,
        spark_executor_instances=spark_job.spark_executor_instances,
        spark_driver_cores=spark_job.spark_driver_cores,
        spark_executor_cores=spark_job.spark_executor_cores,
        # TODO change naming
        source_system=spark_job.source_system,
    )
    # TODO change naming
    run_his_to_int = JobOperator(job=his_to_int, task_id=f"run-{job_name}")
    # TODO change naming
    watch_his_to_int = JobSensor(
        job_name=his_to_int.metadata.name,
        task_id=f"watch-{job_name}",
        namespace=NAMESPACE,
        poke_interval=60 + job_sensor_poke_jitter(),
    )
    start_node >> run_his_to_int >> watch_his_to_int >> end_node


def job_sensor_poke_jitter(max_jitter_seconds: float = 60) -> float:
    """
    Return random jitter for the job sensor poke interval in an attempt to decrease concurrent requests to the API
    server. Jitter is sampled from a uniform distribution between 0 and max_jitter_seconds.
    """
    return uniform(0, max_jitter_seconds)  # nosec
