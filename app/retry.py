import asyncio
import logging
from app.config import settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BASE_DELAY = 2.0
MAX_DELAY = 30.0


def get_backoff_delay(retry_count: int) -> float:
    delay = min(BASE_DELAY ** retry_count, MAX_DELAY)
    logger.info(f"[Retry] Backoff delay: {delay}s (attempt {retry_count})")
    return delay


async def with_retry(task, process_fn, queue_store, broker):
    from app.models import TaskState
    from app.persistence import update_task_state
    from app.metrics import TASKS_RETRIED, TASKS_DLQ, TASKS_FAILED

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await process_fn(task)
            return True
        except Exception as e:
            logger.error(f"[Retry] Task {task.id} attempt {attempt} failed: {e}")
            task.retry_count = attempt
            task.error = str(e)

            if attempt < MAX_RETRIES:
                TASKS_RETRIED.labels(priority=task.priority.name).inc()
                delay = get_backoff_delay(attempt)
                logger.info(f"[Retry] Retrying task {task.id} in {delay}s...")
                await asyncio.sleep(delay)
            else:
                # Send to DLQ
                logger.error(f"[Retry] Task {task.id} exhausted retries — sending to DLQ.")
                TASKS_DLQ.labels(priority=task.priority.name).inc()
                TASKS_FAILED.labels(priority=task.priority.name).inc()
                task.state = TaskState.FAILED
                queue_store.update(task)
                await update_task_state(task)

                await broker.publish_dlq({
                    "id": task.id,
                    "payload": task.payload,
                    "priority": task.priority.name,
                    "error": str(e),
                    "retry_count": task.retry_count,
                })
                return False
    return False
