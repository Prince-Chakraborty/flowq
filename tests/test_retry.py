import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.models import Task, TaskPriority, TaskState
from app.queue import TaskQueue
from app.retry import with_retry, MAX_RETRIES


@pytest.mark.asyncio
async def test_retry_success_on_first_attempt():
    task = Task(payload={}, priority=TaskPriority.HIGH)
    queue_store = TaskQueue()
    queue_store.push(task)
    broker = MagicMock()
    broker.publish_dlq = AsyncMock()

    process_fn = AsyncMock()
    result = await with_retry(task, process_fn, queue_store, broker)
    assert result is True
    assert process_fn.call_count == 1


@pytest.mark.asyncio
async def test_retry_exhausted_sends_to_dlq():
    task = Task(payload={}, priority=TaskPriority.HIGH)
    queue_store = TaskQueue()
    queue_store.push(task)
    broker = MagicMock()
    broker.publish_dlq = AsyncMock()

    process_fn = AsyncMock(side_effect=Exception("always fails"))

    with patch("asyncio.sleep", new_callable=AsyncMock), \
         patch("app.persistence.update_task_state", new_callable=AsyncMock):
        result = await with_retry(task, process_fn, queue_store, broker)

    assert result is False
    assert process_fn.call_count == MAX_RETRIES
    assert broker.publish_dlq.called
    assert task.state == TaskState.FAILED
