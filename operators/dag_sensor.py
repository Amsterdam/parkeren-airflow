import logging

from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State

logger = logging.getLogger("airflow.dag_sensor")


class DagSensor(BaseSensorOperator):
    """
    Sensor that check if a Dag is currently running.
    It proceeds only if the Dag is in not running.
    """

    def __init__(self, external_dag_id, **kwargs):
        super(DagSensor, self).__init__(**kwargs)
        self.external_dag_id = external_dag_id

    @provide_session
    def poke(self, context, session=None):
        dag_run = DagRun

        count = (
            session.query(dag_run)
            .filter(
                dag_run.dag_id == self.external_dag_id,
                dag_run._state.in_([State.RUNNING]),
            )
            .count()
        )
        session.commit()
        session.close()

        logger.info(f"Dag {self.external_dag_id} in running status: {count}")

        if count > 0:
            return False
        else:
            return True
