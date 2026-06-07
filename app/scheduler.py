import asyncio
import logging
import time
from app.config import settings

logger = logging.getLogger(__name__)


class TaskScheduler:
    """
    Polls Redis sorted set for scheduled tasks.
    Tasks scheduled for future execution are stored with
    their execution timestamp as the score.
    When the time comes, they are published to RabbitMQ.
    """

    def __init__(self, redis_client, broker, queue_store):
        self.redis = redis_client
        self.broker = broker
        self.queue_store = queue_store
        self._running = False
        self._task = None
        self.SCHEDULED_KEY = "flowq:scheduled"

    async def schedule(self, task_data: dict, run_at: float) -> None:
        """Add task to scheduled sorted set."""
        import json
        await self.redis.zadd(
            self.SCHEDULED_KEY,
            {json.dumps(task_data): run_at}
        )
        logger.info(f"[Scheduler] Task {task_data['id']} scheduled for {run_at}")

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._poll())
        logger.info("[Scheduler] Started.")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("[Scheduler] Stopped.")

    async def _poll(self):
        """Every second, check for tasks ready to execute."""
        import json
        while self._running:
            try:
                now = time.time()
                # Atomically get all tasks due now
                due_tasks = await self.redis.zrangebyscore(
                    self.SCHEDULED_KEY, 0, now
                )
                for raw in due_tasks:
                    task_data = json.loads(raw)
                    # Remove from scheduled set atomically
                    removed = await self.redis.zrem(self.SCHEDULED_KEY, raw)
                    if removed:
                        await self.broker.publish(task_data)
                        logger.info(f"[Scheduler] Released task {task_data['id']} to queue.")
            except Exception as e:
                logger.error(f"[Scheduler] Poll error: {e}")
            await asyncio.sleep(1)
