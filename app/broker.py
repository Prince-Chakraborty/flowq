import aio_pika
import json
import logging
from app.config import settings

logger = logging.getLogger(__name__)


class RabbitMQBroker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.dlq = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=settings.WORKER_COUNT)

        self.dlq = await self.channel.declare_queue(
            settings.DLQ_NAME,
            durable=True,
        )

        self.queue = await self.channel.declare_queue(
            settings.QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": settings.DLQ_NAME,
            },
        )
        logger.info(f"[Broker] Connected. Queue: {settings.QUEUE_NAME} | DLQ: {settings.DLQ_NAME}")

    async def publish(self, task_data: dict):
        if not self.channel:
            raise RuntimeError("Broker not connected.")
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(task_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=settings.QUEUE_NAME,
        )
        logger.info(f"[Broker] Published task {task_data['id']}")

    async def publish_dlq(self, task_data: dict):
        if not self.channel:
            raise RuntimeError("Broker not connected.")
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(task_data).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=settings.DLQ_NAME,
        )
        logger.info(f"[Broker] Task {task_data['id']} sent to DLQ.")

    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("[Broker] Connection closed.")


broker = RabbitMQBroker()
