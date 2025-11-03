from __future__ import annotations
from airflow import DAG
from airflow.sdk import task
from datetime import datetime, timedelta
from sensors.mongo_change_stream import MongoChangeStreamSensor

# Ejemplo: despertamos cuando haya insert/update en la colección "orders"
# y luego procesamos el cambio.

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="mongo_change_stream_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,              # típico patrón event-driven (sin cron)
    catchup=False,
    default_args=default_args,
    tags=["mongo", "change-stream", "event-driven"],
) as dag:

    listen_orders = MongoChangeStreamSensor(
        task_id="wait_for_order_change",
        mongo_conn_id="mongo_default",
        database="mydb",
        collection="orders",
        # Pipeline opcional: filtra tipos de operación/colección/fields
        pipeline=[
            {"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}},
        ],
        full_document="updateLookup",    # útil para tener documento completo tras update
        max_wait_seconds=3600,           # 1 hora de espera antes de timeout
        # start_at_operation_time=datetime.utcnow(),  # opcional
        # resume_after={"_data": "..."}               # opcional (resume token)
    )

    @task
    def process_change(event: dict):
        """
        'event' llega del XCom del sensor. Ejemplo simple de uso.
        """
        op = event.get("operationType")
        doc = event.get("fullDocument")
        doc_key = event.get("documentKey")
        ns = event.get("ns", {})
        print(f"[ChangeStream] op={op} ns={ns} docKey={doc_key}")
        if doc:
            print("Documento completo:", doc)
        # Aquí harías tu lógica: invalidaciones, ingestas, disparar otros sistemas, etc.

    process_change(listen_orders.output)
