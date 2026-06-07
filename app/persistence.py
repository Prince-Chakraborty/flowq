import logging
from sqlalchemy import select, update
from app.database import AsyncSessionLocal
from app.models import Task, TaskRecord

logger = logging.getLogger(__name__)


async def save_task(task: Task) -> None:
    async with AsyncSessionLocal() as session:
        record = TaskRecord(
            id=task.id,
            payload=task.payload,
            priority=task.priority.name,
            state=task.state.value,
            created_at=task.created_at,
            scheduled_at=task.scheduled_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            error=task.error,
            retry_count=task.retry_count,
        )
        session.add(record)
        await session.commit()
        logger.info(f"[DB] Saved task {task.id}")


async def update_task_state(task: Task) -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(TaskRecord)
            .where(TaskRecord.id == task.id)
            .values(
                state=task.state.value,
                started_at=task.started_at,
                completed_at=task.completed_at,
                error=task.error,
                retry_count=task.retry_count,
            )
        )
        await session.commit()
        logger.info(f"[DB] Updated task {task.id} → {task.state.value}")


async def get_all_tasks() -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(TaskRecord))
        records = result.scalars().all()
        return [
            {
                "id": r.id,
                "payload": r.payload,
                "priority": r.priority,
                "state": r.state,
                "created_at": r.created_at,
                "scheduled_at": r.scheduled_at,
                "started_at": r.started_at,
                "completed_at": r.completed_at,
                "error": r.error,
                "retry_count": r.retry_count,
            }
            for r in records
        ]
