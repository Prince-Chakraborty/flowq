import asyncio
import json
import logging
import time
from aio_pika.abc import AbstractIncomingMessage
from app.models import Task, TaskState, TaskPriority
from app.persistence import update_task_state
from app.lock import acquire_lock, release_lock, heartbeat, set_visibility_timeout, clear_visibility
from app.retry import with_retry
from app.config import settings
from app.metrics import (
    TASKS_COMPLETED, TASKS_FAILED,
    TASK_PROCESSING_LATENCY, ACTIVE_WORKERS, QUEUE_LENGTH
)

logger = logging.getLogger(__name__)

# Configurable concurrency limit per node
_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_JOBS)


async def process_task(task: Task) -> None:
    logger.info(f"[Worker] Processing task {task.id} | payload: {task.payload}")
    await asyncio.sleep(1)  # Simulate work
    logger.info(f"[Worker] Completed task {task.id}")


async def _heartbeat_loop(task_id: str, stop_event: asyncio.Event):
    """Continuously extend visibility timeout while task is running."""
    interval = settings.VISIBILITY_TIMEOUT // 2
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        if not stop_event.is_set():
            alive = await heartbeat(task_id)
            if not alive:
                logger.warning(f"[Heartbeat] Lost lock for task {task_id} — may have timed out.")
                break


async def handle_message(message: AbstractIncomingMessage, queue_store, broker) -> None:
    async with _semaphore:
        async with message.process(requeue=False):
            data = json.loads(message.body.decode())
            task_id = data["id"]

            # Atomic Redis Lua lock — prevent duplicate processing
            if not await acquire_lock(task_id):
                logger.warning(f"[Worker] Skipping duplicate task {task_id}")
                return

            # Set visibility timeout
            await set_visibility_timeout(task_id)

            task = queue_store.get(task_id)
            if not task:
                task = Task(
                    payload=data["payload"],
                    priority=TaskPriority[data["priority"]],
                )
                task.id = task_id

            # Start heartbeat to extend visibility timeout
            stop_heartbeat = asyncio.Event()
            heartbeat_task = asyncio.create_task(
                _heartbeat_loop(task_id, stop_heartbeat)
            )

            try:
                task.state = TaskState.RUNNING
                task.started_at = time.time()
                queue_store.update(task)
                await update_task_state(task)

                ACTIVE_WORKERS.inc()
                start_time = time.time()

                success = await with_retry(task, process_task, queue_store, broker)

                elapsed = time.time() - start_time
                TASK_PROCESSING_LATENCY.labels(priority=task.priority.name).observe(elapsed)
                ACTIVE_WORKERS.dec()
                QUEUE_LENGTH.dec()

                if success:
                    task.state = TaskState.COMPLETED
                    task.completed_at = time.time()
                    queue_store.update(task)
                    await update_task_state(task)
                    TASKS_COMPLETED.labels(priority=task.priority.name).inc()

            except Exception as e:
                ACTIVE_WORKERS.dec()
                logger.error(f"[Worker] Unhandled error for task {task_id}: {e}")
                task.state = TaskState.FAILED
                task.error = str(e)
                task.completed_at = time.time()
                queue_store.update(task)
                await update_task_state(task)
                TASKS_FAILED.labels(priority=task.priority.name).inc()

            finally:
                # Stop heartbeat
                stop_heartbeat.set()
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
                await clear_visibility(task_id)
                await release_lock(task_id)


class WorkerPool:
    def __init__(self, broker, queue_store):
        self.broker = broker
        self.queue_store = queue_store
        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self):
        self._running = True
        logger.info(f"[WorkerPool] Starting {settings.WORKER_COUNT} workers | max concurrent jobs: {settings.MAX_CONCURRENT_JOBS}")
        for i in range(settings.WORKER_COUNT):
            t = asyncio.create_task(self._consume(worker_id=i))
            self._tasks.append(t)

    async def _consume(self, worker_id: int):
        logger.info(f"[Worker-{worker_id}] Started.")
        async with self.broker.queue.iterator() as queue_iter:
            async for message in queue_iter:
                if not self._running:
                    break
                await handle_message(message, self.queue_store, self.broker)

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("[WorkerPool] All workers stopped.")
