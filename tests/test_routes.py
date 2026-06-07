import pytest
from httpx import AsyncClient, ASGITransport
from unittest.mock import AsyncMock, patch, MagicMock
from app.main import app
from app.models import Task, TaskPriority, TaskState
from app.queue import TaskQueue


@pytest.fixture
def mock_app_state():
    queue = TaskQueue()
    broker = MagicMock()
    broker.publish = AsyncMock()
    scheduler = MagicMock()
    scheduler.schedule = AsyncMock()
    app.state.queue = queue
    app.state.broker = broker
    app.state.scheduler = scheduler
    app.state.settings = MagicMock(
        MAX_CONCURRENT_JOBS=10,
        WORKER_COUNT=3,
        VISIBILITY_TIMEOUT=60,
    )
    return queue


@pytest.mark.asyncio
async def test_submit_task(mock_app_state):
    with patch("app.routes.save_task", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post("/tasks", json={
                "payload": {"job": "test"},
                "priority": "HIGH"
            })
    assert response.status_code == 202
    data = response.json()
    assert "task_id" in data
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_submit_task_invalid_priority(mock_app_state):
    with patch("app.routes.save_task", new_callable=AsyncMock):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post("/tasks", json={
                "payload": {"job": "test"},
                "priority": "INVALID"
            })
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_task_not_found(mock_app_state):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/tasks/nonexistent-id")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_task_found(mock_app_state):
    task = Task(payload={"job": "test"}, priority=TaskPriority.HIGH)
    mock_app_state.push(task)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(f"/tasks/{task.id}")
    assert response.status_code == 200
    assert response.json()["id"] == task.id


@pytest.mark.asyncio
async def test_health_check():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_queue_stats(mock_app_state):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/queue/stats")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "pending" in data
