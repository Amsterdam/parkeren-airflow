from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from kubernetes.client.rest import ApiException

try:
    from kubernetes import client, config

except ImportError:
    raise ImportError("error importing kubernetes module")


class Kuberneteshook(BaseHook):
    """
    Interact with kubernetes API.
    :param kube_config: kubeconfig - kubernetes connection string
    :type kube_config: str
    """

    def __init__(self, kube_config=None):
        super().__init__()
        self.kube_config = kube_config

    def get_conn(self, api_kind=None):
        """
        Returns kubernetes api session for use with requests
        :param api_kind: kubernetes api type
        :type api_kind: string
        """
        try:
            config.load_incluster_config()
        except config.ConfigException:
            if self.kube_config:
                config.load_kube_config(self.kube_config)
            else:
                config.load_kube_config()

        if not api_kind == "crd" and not api_kind == "configmap":
            raise NotImplementedError()

        if api_kind == "crd":
            return client.CustomObjectsApi()
        else:
            return client.CoreV1Api()

    def create_custom_resource_definition(
        self,
        group=None,
        version=None,
        namespace="default",
        plural=None,
        crd_object=None,
    ):
        r"""
        creates CRD object in kubernetes
        :param plural:
        :param version:
        :param group:
        :param crd_object: crd object definition
        :type crd_object: str
        :param namespace: kubernetes namespace
        :type namespace: str
        """
        api = self.get_conn("crd")
        try:
            response = api.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                body=crd_object,
            )
            self.log.info(response)
            return response
        except ApiException as e:
            raise AirflowException(
                "Exception when calling -> create_custom_resource_definition: %s\n" % e
            )

    def get_custom_resource_definition(
        self, group=None, version=None, namespace="default", plural=None, name=None
    ):
        api = self.get_conn("crd")
        try:
            crd = api.get_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name,
            )
            return crd
        except ApiException as e:
            raise AirflowException(
                "Exception when calling -> get_custom_resource_definition: %s\n" % e
            )

    def delete_custom_resource_definition(
        self, group=None, version=None, namespace="default", plural=None, name=None
    ):
        api = self.get_conn("crd")
        try:
            crd = api.delete_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name,
            )
            return crd
        except ApiException as e:
            raise AirflowException(
                "Exception when calling -> delete_custom_resource_definition: %s\n" % e
            )

    def create_config_map(self, name=None, namespace="default", data=None):
        r"""
        creates ConfigMap in kubernetes
        :param name: config map name
        :type name: str
        :param namespace: kubernetes namespace
        :type namespace: str
        :param data: data to be set in the config map
        :type namespace: dict(str, str)
        """
        api = self.get_conn("configmap")
        try:
            body = client.V1ConfigMap(
                data=data, metadata=client.V1ObjectMeta(name=name, namespace=namespace)
            )
            response = api.create_namespaced_config_map(namespace=namespace, body=body)
            return response
        except ApiException as e:
            raise AirflowException(
                "Exception when calling -> create_config_map: %s\n" % e
            )

    def get_config_map(self, name=None, namespace="default"):
        api = self.get_conn("configmap")
        try:
            config_map = api.read_namespaced_config_map(name=name, namespace=namespace)
            return config_map
        except ApiException as e:
            raise AirflowException("Exception when calling -> get_config_map: %s\n" % e)

    def delete_config_map(self, name=None, namespace="default"):
        api = self.get_conn("configmap")
        try:
            config_map = api.delete_namespaced_config_map(
                name=name, namespace=namespace
            )
            return config_map
        except ApiException as e:
            raise AirflowException(
                "Exception when calling -> delete_config_map: %s\n" % e
            )
