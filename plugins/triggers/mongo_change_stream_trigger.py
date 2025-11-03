from __future__ import annotations
from typing import Any, Dict, List, Optional
from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio
import json
import datetime as dt

class MongoChangeStreamTrigger(BaseTrigger):
    """
    Trigger asíncrono que escucha un Change Stream de MongoDB usando Motor.
    Emite un TriggerEvent en cuanto llega el primer evento que cumpla el pipeline.
    """
    def __init__(
        self,
        mongo_uri: str,
        database: str,
        collection: str,
        pipeline: Optional[List[Dict[str, Any]]] = None,
        full_document: str = "default",
        max_wait_seconds: int = 3600,
        start_at_operation_time: Optional[float] = None,  # epoch seconds
        resume_after: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__()
        self.mongo_uri = mongo_uri
        self.database = database
        self.collection = collection
        self.pipeline = pipeline or []
        self.full_document = full_document  # "default" | "updateLookup" | "whenAvailable" (Mongo >= 6)
        self.max_wait_seconds = max_wait_seconds
        self.start_at_operation_time = start_at_operation_time
        self.resume_after = resume_after
    
    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            {
                "mongo_uri": self.mongo_uri,
                "database": self.database,
                "collection": self.collection,
                "pipeline": self.pipeline,
                "full_document": self.full_document,
                "max_wait_seconds": self.max_wait_seconds,
                "start_at_operation_time": self.start_at_operation_time,
                "resume_after": self.resume_after,
            },
        )

    async def run(self):
        # Se ejecuta en el Triggerer (entorno async)
        from motor.motor_asyncio import AsyncIOMotorClient
        client = AsyncIOMotorClient(self.mongo_uri)
        db = client[self.database]
        coll = db[self.collection]

        watch_kwargs: Dict[str, Any] = {
            "full_document": self.full_document
        }
        if self.start_at_operation_time:
            # Convertimos epoch seconds -> Timestamp para Mongo
            watch_kwargs["start_at_operation_time"] = dt.datetime.fromtimestamp(
                self.start_at_operation_time, tz=dt.timezone.utc
            )
        if self.resume_after:
            watch_kwargs["resume_after"] = self.resume_after

        deadline = asyncio.get_event_loop().time() + self.max_wait_seconds

        try:
            async with coll.watch(self.pipeline, **watch_kwargs) as stream:
                while True:
                    timeout = max(0, deadline - asyncio.get_event_loop().time())
                    if timeout == 0:
                        yield TriggerEvent({
                            "status": "timeout",
                            "message": f"Sin eventos en {self.max_wait_seconds}s",
                        })
                        return

                    try:
                        change = await asyncio.wait_for(stream.next(), timeout=timeout)
                        # Emitimos el primer evento y terminamos
                        event = {
                            "status": "event",
                            "operationType": change.get("operationType"),
                            "documentKey": change.get("documentKey"),
                            "fullDocument": change.get("fullDocument"),
                            "ns": change.get("ns"),
                            "_id": change.get("_id"),
                        }
                        # Nota: _id del change stream es el resumeToken, útil para retomar
                        yield TriggerEvent(event)
                        return
                    except asyncio.TimeoutError:
                        yield TriggerEvent({
                            "status": "timeout",
                            "message": f"Sin eventos en {self.max_wait_seconds}s",
                        })
                        return
        finally:
            client.close()
