import yaml
from dataclasses import dataclass, field
from typing import List

from operators.kubernetes_hook import Kuberneteshook
from airflow.models import BaseOperator


@dataclass
class EnvironmentRef:
    """Definition to retrieve a environment var from a secret."""

    variable_name: str
    secret_reference: str
    secret_key: str


@dataclass
class EnvironmentVar:
    """Definition of a environment variable."""

    name: str
    value: str


@dataclass
class Resource:
    """Kubernetes resource requirements."""

    cores: int = 1
    core_limit: str = "1000m"
    memory: str = "512m"
    instances: int = 1  # only applies to executors


@dataclass
class SparkApplication:
    """Mapper for the SparkApplication CRD."""

    image: str
    application_path: str
    minio_endpoint: str
    arguments: List[str] = field(default_factory=list)
    image_pull_secrets: List[str] = field(default_factory=list)
    environment_vars: List[EnvironmentVar] = field(default_factory=list)
    environment_refs: List[EnvironmentRef] = field(default_factory=list)
    config_refs: List[str] = field(default_factory=list)
    secret_refs: List[str] = field(default_factory=list)
    driver_resources: Resource = Resource()
    executor_resources: Resource = Resource()
    namespace: str = "default"
    service_account: str = "airflow"


class SparkKubernetesOperator(BaseOperator):
    """
    creates sparkApplication in kubernetes cluster
    """

    template_fields = ["job_id", "crd", "kube_config"]
    template_ext = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        job_id: str,
        crd: SparkApplication = None,
        kube_config: str = "",
        crd_xcom_key: str = "",
        crd_task_id: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.crd = crd
        self.kube_config = kube_config

        if crd is None:
            if "crd_xcom_key" != "" and "crd_task_id" != "":
                self.crd_xcom_key = crd_xcom_key
                self.crd_task_id = crd_task_id
            else:
                raise RuntimeError(
                    "If no SparkApplication object is supplied you must provide args 'crd_task_id' and "
                    "'crd_xcom_key' referring to an Airflow task that generates a SparkApplication and "
                    "stores it in an XCom with that key"
                )

    def execute(self, context):
        if self.crd is None:
            ti = context["task_instance"]
            self.crd = ti.xcom_pull(key=self.crd_xcom_key, task_ids=self.crd_task_id)

        kubernetes_hook = Kuberneteshook()
        kubernetes_hook.create_config_map(
            name=f"log4j2-{self.job_id}",
            namespace=self.crd.namespace,
            data=self.get_log4j2_properties(),
        )
        kubernetes_hook.create_custom_resource_definition(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.crd.namespace,
            plural="sparkapplications",
            crd_object=self.generate_yaml(),
        )

    def get_log4j2_properties(self):
        return {
            "log4j2.properties": """|
# Set everything to be logged to the console
appenders = console
appender.console.type = Console
appender.console.name = stderr
appender.console.target = System.err
appender.console.json.type = JsonTemplateLayout
appender.console.json.eventTemplateUri = classpath:LogstashJsonEventLayoutV1.json

rootLogger.level = INFO
rootLogger.appenderRef.stdout.ref = stderr

logger.spark.name = org.apache.spark
logger.spark.level = WARN
logger.spark.additivity = false
logger.spark.appenderRef.stdout.ref = stderr

## Set the default spark-shell log level to WARN. When running the spark-shell, the
## log level for this class is used to overwrite the root logger's log level, so that
## the user can have different defaults for the shell and regular Spark apps.
logger.spark.repl.Main.level = WARN
logger.spark.repl.SparkIMain$exprTyper.level = INFO
logger.spark.repl.SparkILoop$SparkILoopInterpreter.level = INFO

## Settings to quiet third party logs that are too verbose
logger.jetty.name = org.sparkproject.jetty
logger.jetty.level = WARN
logger.jetty.util.component.AbstractLifeCycle.level = ERROR

logger.parquet.name = org.apache.parquet
logger.parquet.level = ERROR

## SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
logger.hadoop.name = org.apache.hadoop
logger.hadoop.level = WARN
logger.hadoop.hive.metastore.RetryingHMSHandler.level = FATAL
logger.hadoop.hive.ql.exec.FunctionRegistry.level = ERROR
            """
        }

    def generate_yaml(self):
        # Grabs just the image name without registry, which is what DataDog defaults to for source and service values
        base_image_name = self.crd.image.rpartition("/")[-1]
        spark_app_yaml = f"""
          apiVersion: "sparkoperator.k8s.io/v1beta2"
          kind: SparkApplication
          metadata:
            name: {self.job_id}
            namespace: {self.crd.namespace}
          spec:
            sparkConfigMap: log4j2-{self.job_id}
            mode: cluster
            type: Python
            pythonVersion: "3"
            image: "{self.crd.image}"
            sparkVersion: "3.0.0"
            imagePullPolicy: Always
            mainApplicationFile: "{self.crd.application_path}"
            timeToLiveSeconds: 86400
            hadoopConf:
              "fs.s3a.endpoint": "{self.crd.minio_endpoint}"
              "fs.s3a.path.style.access": "true"
              "fs.s3a.connection.ssl.enabled": "false"
              "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
            restartPolicy:
              type: Never
            driver:
              annotations:
                ad.datadoghq.com/spark-kubernetes-driver.logs: |
                  [{{
                    "source": "{base_image_name}",
                    "service": "{base_image_name}-{self.crd.namespace}",
                    "log_processing_rules": [{{
                      "type": "exclude_at_match",
                      "name": "ignore_entrypoint_sh_commands",
                      "pattern": "^\\\\+(.*)$"
                    }},
                    {{
                      "type": "multi_line",
                      "name": "starts_with_date_time",
                      "pattern": "(\\\\d{{2}}\\/\\\\d{{2}}\\/\\\\d{{2}}\\\\s\\\\d{{2}}:\\\\d{{2}}:\\\\d{{2}})"
                    }}]
                  }}]
              cores: {self.crd.driver_resources.cores}
              coreLimit: "{self.crd.driver_resources.core_limit}"
              memory: "{self.crd.driver_resources.memory}"
              serviceAccount: {self.crd.service_account}
              labels:
                version: 3.0.0
            executor:
              cores: {self.crd.executor_resources.cores}
              instances: {self.crd.executor_resources.instances}
              memory: "{self.crd.executor_resources.memory}"
              labels:
                version: 3.0.0
        """

        app_yaml = yaml.safe_load(spark_app_yaml)
        app_yaml = self._add_environment_vars_via_secret(app_yaml)
        app_yaml = self._add_arguments(app_yaml)
        app_yaml = self._add_config_and_secret_refs(app_yaml)
        app_yaml = self._add_image_pull_secrets(app_yaml)

        return app_yaml

    def _add_environment_vars_via_secret(self, app_yaml):
        envs = []
        for env in self.crd.environment_refs:
            envs.append(
                {
                    "name": env.variable_name,
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": env.secret_reference,
                            "key": env.secret_key,
                        }
                    },
                }
            )

        for env_var in self.crd.environment_vars:
            envs.append({"name": env_var.name, "value": env_var.value})

        app_yaml["spec"]["driver"].update({"env": envs})
        app_yaml["spec"]["executor"].update({"env": envs})
        return app_yaml

    def _add_arguments(self, app_yaml):
        app_yaml["spec"].update({"arguments": self.crd.arguments})
        return app_yaml

    def _add_config_and_secret_refs(self, app_yaml):
        refs = []
        for config in self.crd.config_refs:
            refs.append({"configMapRef": {"name": config}})

        for secret in self.crd.secret_refs:
            refs.append({"secretRef": {"name": secret}})

        app_yaml["spec"]["driver"].update({"envFrom": refs})
        app_yaml["spec"]["executor"].update({"envFrom": refs})
        return app_yaml

    def _add_image_pull_secrets(self, app_yaml):
        app_yaml["spec"].update({"imagePullSecrets": self.crd.image_pull_secrets})
        return app_yaml
