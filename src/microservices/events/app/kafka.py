import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
import time

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "cinemaabyss.events"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # будет переопределяться из env

producer: AIOKafkaProducer | None = None
consumer_task: asyncio.Task | None = None


async def init_kafka_producer():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await producer.start()
    logger.info("✅ Kafka producer started")


async def send_event_to_kafka(event: dict):
    if not producer:
        raise RuntimeError("Kafka producer not initialized")
    future = await producer.send_and_wait(KAFKA_TOPIC, value=event)
    return {"partition": future.topic_partition.partition, "offset": future.offset}


async def consume_events():
    """Фоновая задача: читает события из Kafka и логирует их."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="events-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for msg in consumer:
            logger.info(f"[KAFKA CONSUMER] Received event: {msg.value}")
    finally:
        await consumer.stop()


async def start_kafka_consumer():
    global consumer_task
    # Ждём, пока Kafka поднимется (в реальном проекте — health check)
    await asyncio.sleep(5)
    consumer_task = asyncio.create_task(consume_events())


async def stop_kafka():
    global producer, consumer_task
    if consumer_task:
        consumer_task.cancel()
    if producer:
        await producer.stop()