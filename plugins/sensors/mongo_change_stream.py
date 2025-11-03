from __future__ import annotations
from typing import Any, Dict, List, Optional
from airflow.sdk.bases.sensor import BaseSensorOperator      # antes: airflow.sensors.base
from airflow.exceptions import AirflowException              # igual
from airflow.utils.context import Context                    # igual
from airflow.sdk.bases.hook import BaseHook                  # antes: airflow.hooks.base
from airflow.models.xcom import XCom                         # igual (no deprecated)
from datetime import datetime, timezone

from triggers.mongo_change_stream_trigger import MongoChangeStreamTrigger

class MongoChangeStreamSensor(BaseSensorOperator):
    """
    Sensor diferible: se duerme y delega la espera al Triggerer (barato).
    Despierta cuando el Trigger emite un evento del Change Stream.
    """
    def __init__(
        self,
        *,
        mongo_conn_id: str = "mongo_default",
        database: str,
        collection: str,
        pipeline: Optional[List[Dict[str, Any]]] = None,
        full_document: str = "default",
        max_wait_seconds: int = 3600,
        start_at_operation_time: Optional[datetime] = None,
        resume_after: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.database = database
        self.collection = collection
        self.pipeline = pipeline or []
        self.full_document = full_document
        self.max_wait_seconds = max_wait_seconds
        self.start_at_operation_time = start_at_operation_time
        self.resume_after = resume_after

        # Modo diferible (Airflow >=2.7 soporta deferrable=True en sensores base)
        self.deferrable = True

    def _build_uri_from_conn(self) -> str:
        conn = BaseHook.get_connection(self.mongo_conn_id)
        # Si hay URI en la conn, úsala
        if conn.get_uri():
            return conn.get_uri()

        # Construcción manual (básico)
        auth = ""
        if conn.login and conn.password:
            auth = f"{conn.login}:{conn.password}@"
        host = conn.host or "localhost"
        port = f":{conn.port}" if conn.port else ""
        schema = conn.schema or ""
        return f"mongodb://{auth}{host}{port}/{schema}"

    def execute(self, context: Context) -> Any:
        mongo_uri = self._build_uri_from_conn()
        start_epoch = None
        if self.start_at_operation_time:
            start_epoch = self.start_at_operation_time.replace(tzinfo=timezone.utc).timestamp()

        trigger = MongoChangeStreamTrigger(
            mongo_uri=mongo_uri,
            database=self.database,
            collection=self.collection,
            pipeline=self.pipeline,
            full_document=self.full_document,
            max_wait_seconds=self.max_wait_seconds,
            start_at_operation_time=start_epoch,
            resume_after=self.resume_after,
        )

        # Se suspende la tarea hasta recibir el TriggerEvent
        self.defer(trigger=trigger, method_name="execute_complete")

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> Any:
        """
        Se llama cuando el Trigger emite TriggerEvent.
        Devuelve el evento (va a XCom) o lanza excepción si timeout.
        """
        if not event:
            raise AirflowException("MongoChangeStreamSensor: evento vacío del Trigger.")
        status = event.get("status")
        if status == "timeout":
            self.log.info("Timeout: %s", event.get("message"))
            raise AirflowException(event.get("message", "Timeout sin eventos."))

        self.log.info("Evento recibido: %s", event)
        # Devolvemos el evento para XCom (downstream puede leerlo)
        return event
