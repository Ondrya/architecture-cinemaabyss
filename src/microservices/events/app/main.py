import os
import logging
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from .kafka import (
    init_kafka_producer,
    start_kafka_consumer,
    send_event_to_kafka,
    stop_kafka,
    KAFKA_BOOTSTRAP_SERVERS
)
from .models import (
    MovieEventPayload,
    UserEventPayload,
    PaymentEventPayload,
    EventResponse
)
from uuid import uuid4
from datetime import datetime

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("events-service")

# Поддержка переменной окружения
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_kafka_producer()
    await start_kafka_consumer()
    yield
    # Shutdown
    await stop_kafka()


app = FastAPI(
    title="CinemaAbyss Events Service",
    description="MVP сервис для отправки и потребления событий через Kafka",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/api/events/health")
async def health_check():
    return {"status": True}


@app.post("/api/events/movie", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def create_movie_event(payload: MovieEventPayload):
    try:
        event = {
            "id": f"movie-{payload.movie_id}-{payload.action}-{uuid4()}",
            "type": "movie",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "payload": payload.model_dump()
        }
        meta = await send_event_to_kafka(event)
        return EventResponse(
            partition=meta["partition"],
            offset=meta["offset"],
            event=event
        )
    except Exception as e:
        logger.error(f"Error sending movie event: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error"}
        )


@app.post("/api/events/user", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def create_user_event(payload: UserEventPayload):
    try:
        event = {
            "id": f"user-{payload.user_id}-{payload.action}-{uuid4()}",
            "type": "user",
            "timestamp": payload.timestamp.isoformat() + "Z",
            "payload": payload.model_dump()
        }
        meta = await send_event_to_kafka(event)
        return EventResponse(
            partition=meta["partition"],
            offset=meta["offset"],
            event=event
        )
    except Exception as e:
        logger.error(f"Error sending user event: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error"}
        )


@app.post("/api/events/payment", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def create_payment_event(payload: PaymentEventPayload):
    try:
        event = {
            "id": f"payment-{payload.payment_id}-{payload.status}-{uuid4()}",
            "type": "payment",
            "timestamp": payload.timestamp.isoformat() + "Z",
            "payload": payload.model_dump()
        }
        meta = await send_event_to_kafka(event)
        return EventResponse(
            partition=meta["partition"],
            offset=meta["offset"],
            event=event
        )
    except Exception as e:
        logger.error(f"Error sending payment event: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error"}
        )