from typing import Dict

from airflow.exceptions import AirflowException
from operators.kubernetes_hook import Kuberneteshook
from airflow.sensors.base import BaseSensorOperator


class SparkKubernetesSensor(BaseSensorOperator):
    """
    checks sparkapplication state on kubernetes
    :param sparkapplication_name: sparkapplication resource name
    :type http_conn_id: str
    :param namespace: the kubernetes namespace where the sparkapplication reside in
    :type method: str
    :param kube_config: kubeconfig file location
    :type endpoint: str
    """

    template_fields = ("sparkapplication_name", "namespace", "kube_config")
    INTERMEDIATE_STATES = ("NEW", "SUBMITTED", "RUNNING")
    FAILURE_STATES = ("FAILED", "SUBMISSION_FAILED", "UNKNOWN")
    SUCCESS_STATES = "COMPLETED"

    K8S_GROUP = "sparkoperator.k8s.io"
    K8S_VERSION = "v1beta2"
    k8S_PLURAL = "sparkapplications"

    def __init__(
        self,
        sparkapplication_name: str,
        namespace: str = "default",
        kube_config: str = "",
        timeout: int = 60 * 60 * 2,  # default timeout of 2 hours
        **kwargs,
    ):
        super().__init__(timeout=timeout, **kwargs)
        self.sparkapplication_name = sparkapplication_name
        self.namespace = namespace
        self.kube_config = kube_config
        self.hook = Kuberneteshook()

    def poke(self, context: Dict):
        self.log.info("Poking: %s", self.sparkapplication_name)

        crd_state = self.hook.get_custom_resource_definition(
            group=self.K8S_GROUP,
            version=self.K8S_VERSION,
            namespace=self.namespace,
            plural=self.k8S_PLURAL,
            name=self.sparkapplication_name,
        )

        while "status" not in crd_state:
            self.log.info("CRD has not been picked up by the Spark operator yet")
            return False

        sparkapplication_state = crd_state["status"]["applicationState"]["state"]

        self.log.info("spark application state: {}".format(sparkapplication_state))
        if sparkapplication_state in self.FAILURE_STATES:
            self.cleanup(delete_crd=False)
            raise AirflowException(
                "spark application failed with state: {}".format(sparkapplication_state)
            )
        if sparkapplication_state in self.INTERMEDIATE_STATES:
            self.log.info(
                "spark application is still in state: {}".format(sparkapplication_state)
            )
            return False
        if sparkapplication_state in self.SUCCESS_STATES:
            self.log.info("spark application ended successfully")
            self.cleanup()
            return True
        raise AirflowException(
            "unknown spark application state: {}".format(sparkapplication_state)
        )

    def cleanup(self, delete_crd: bool = True):
        self.log.info("Cleaning up resources")
        # Always delete log4j2 configmap regardless of success to prevent clogging up cluster
        self.hook.delete_config_map(
            namespace=self.namespace, name=f"log4j2-{self.sparkapplication_name}"
        )
        if delete_crd:
            self.hook.delete_custom_resource_definition(
                group=self.K8S_GROUP,
                version=self.K8S_VERSION,
                namespace=self.namespace,
                plural=self.k8S_PLURAL,
                name=self.sparkapplication_name,
            )
