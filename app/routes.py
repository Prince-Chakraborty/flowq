from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Optional
import time
import logging
from app.models import Task, TaskPriority, TaskState
from app.persistence import save_task, get_all_tasks
from app.metrics import TASKS_SUBMITTED, QUEUE_LENGTH

router = APIRouter()
logger = logging.getLogger(__name__)


class TaskSubmitRequest(BaseModel):
    payload: Any
    priority: Optional[str] = "MEDIUM"
    delay_seconds: Optional[int] = None


@router.post("/tasks", status_code=202)
async def submit_task(request: Request, body: TaskSubmitRequest):
    try:
        priority = TaskPriority[body.priority.upper()]
    except KeyError:
        raise HTTPException(status_code=400, detail="Invalid priority. Use HIGH, MEDIUM, or LOW.")

    task = Task(payload=body.payload, priority=priority)

    task_data = {
        "id": task.id,
        "payload": task.payload,
        "priority": task.priority.name,
        "priority_value": task.priority.value,
    }

    if body.delay_seconds and body.delay_seconds > 0:
        task.state = TaskState.SCHEDULED
        task.scheduled_at = time.time() + body.delay_seconds
        request.app.state.queue.push(task)
        await save_task(task)
        await request.app.state.scheduler.schedule(task_data, task.scheduled_at)
        logger.info(f"[Routes] Task {task.id} scheduled in {body.delay_seconds}s")
        return JSONResponse(
            status_code=202,
            content={
                "task_id": task.id,
                "status": task.state.value,
                "scheduled_at": task.scheduled_at,
                "delay_seconds": body.delay_seconds,
            },
            headers={"Location": f"/tasks/{task.id}"},
        )

    request.app.state.queue.push(task)
    await save_task(task)
    await request.app.state.broker.publish(task_data)

    TASKS_SUBMITTED.labels(priority=priority.name).inc()
    QUEUE_LENGTH.inc()

    return JSONResponse(
        status_code=202,
        content={"task_id": task.id, "status": task.state.value},
        headers={"Location": f"/tasks/{task.id}"},
    )


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, request: Request):
    task = request.app.state.queue.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")
    return task.to_dict()


@router.get("/tasks")
async def list_tasks():
    return await get_all_tasks()


@router.get("/queue/stats")
async def queue_stats(request: Request):
    q = request.app.state.queue
    return {
        "total": len(q._store),
        "pending": q.pending_count(),
        "queue_size": q.size(),
        "max_concurrent_jobs": request.app.state.settings.MAX_CONCURRENT_JOBS,
        "worker_count": request.app.state.settings.WORKER_COUNT,
        "visibility_timeout": request.app.state.settings.VISIBILITY_TIMEOUT,
    }
