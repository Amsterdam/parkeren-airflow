from typing import Dict
from time import sleep
from random import uniform

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from kubernetes import config, client
from kubernetes.client.rest import ApiException
from urllib3.exceptions import MaxRetryError


def load_config() -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def isResourceQuotaConflict(e) -> bool:
    """
    Helper function which checks if error e is a ResourceQuota conflict
    A ResourceQuota conflict is hit when the following 3 conditions are met:
    a) Is this a StatusError?
    b) Is this a Conflict?
    c) Is status.Details.Kind == "resourcequotas"
    """
    if e.status != 409:
        return False

    if e.reason != "Conflict":
        return False

    return "resourcequotas" in str(e.body)


class JobOperator(BaseOperator):
    def __init__(self, job: client.V1Job, **kwargs) -> None:
        # TODO: allow dict in addition to V1Job to allow loading from yaml
        super().__init__(**kwargs)

        # TODO: add 'managed-by: airflow' label?
        self.job = job

        # TODO: should ideally be run once; where?
        load_config()

    def execute(self, context):
        self.log.info(f"JobOperator: Submitting job {self.job.metadata.name}")
        max_attempts = 3

        for attempt in range(max_attempts):
            try:
                client.BatchV1Api().create_namespaced_job(
                    namespace=self.job.metadata.namespace, body=self.job
                )
                break
            except Exception as e:
                if isResourceQuotaConflict(e):
                    # add random sleep to spread load to k8 api
                    sleep(uniform(0.2, 1.2))  # nosec
                    self.log.info(
                        f"JobOperator: Retry ({attempt+1}/{max_attempts}) creating Kubernetes job: {e}"
                    )
                else:
                    self.log.info(f"JobOperator: Failed creating Kubernetes job: {e}")
                    break
        else:
            self.log.info(
                f"JobOperator: Failed creating Kubernetes job: reached max retries '{max_attempts}'"
            )


class JobSensor(BaseSensorOperator):
    def __init__(self, job_name: str, namespace: str, **kwargs) -> None:
        super().__init__(**kwargs)

        # TODO: should ideally be run once; where?
        load_config()

        self.job_name = job_name
        self.namespace = namespace

    def poke(self, context: Dict) -> bool:
        # TODO: what is the contract of poke? Can't find it in the docs.
        self.log.info(f"JobSensor: Poking {self.namespace}/{self.job_name}")

        try:
            status = (
                client.BatchV1Api()
                .read_namespaced_job(self.job_name, self.namespace)
                .status
            )
        except ApiException as e:
            if e.status == 404:
                raise AirflowException(
                    f"job {self.namespace}/{self.job_name} not found"
                )
            elif e.status == 500:
                # Assuming internal API/etcd server error, pretend job is running
                return False
            raise
        except MaxRetryError:
            # Reported by Thomas, might also be an API server issue, pretend job is running
            return False

        self.log.info(f"JobSensor: Job status: {status}")

        if self._is_pending_or_running(status):
            return False

        if self._has_failed(status):
            raise AirflowException(f"job {self.namespace}/{self.job_name} has failed")

        if self._has_succeeded(status):
            return True

        raise AirflowException(
            f"job {self.namespace}/{self.job_name} in unknown state: {status}"
        )

    def _is_pending_or_running(self, status: client.V1JobStatus) -> bool:
        """Return whether a job is still pending or running (True) or not
        (False).

        The docs don't make it clear how we can actually check this, but from
        some experiments it seems like the conditions field is only updated
        after failure or completion of a job. In the cases when the job is
        pending or running the field is set to None.
        """
        return status.conditions is None

    def _has_succeeded(self, status: client.V1JobStatus) -> bool:
        """Return whether a job has succeeded (True) or not (False).

        The Kubernetes documentation does not really make it clear when a job
        is supposed to be considered as succeeded. This method checks for the
        presence of a job condition of type 'Complete' and status 'True'.
        """
        for condition in status.conditions:
            if (condition.status == "True") and (condition.type == "Complete"):
                return True

        return False

    def _has_failed(self, status: client.V1JobStatus) -> bool:
        """Return whether a job has failed (True) or not (False).

        This method checks for the presence of a job condition of type 'Failed'
        and status 'True'.
        """
        for condition in status.conditions:
            if (condition.status == "True") and (condition.type == "Failed"):
                return True

        return False
