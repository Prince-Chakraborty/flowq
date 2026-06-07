import pytest
from app.models import Task, TaskState, TaskPriority


def test_task_default_state():
    task = Task(payload={"job": "test"})
    assert task.state == TaskState.PENDING


def test_task_default_priority():
    task = Task(payload={"job": "test"})
    assert task.priority == TaskPriority.MEDIUM


def test_task_priority_ordering():
    high = Task(payload={}, priority=TaskPriority.HIGH)
    low = Task(payload={}, priority=TaskPriority.LOW)
    assert high < low


def test_task_to_dict():
    task = Task(payload={"job": "send_email"}, priority=TaskPriority.HIGH)
    d = task.to_dict()
    assert d["payload"] == {"job": "send_email"}
    assert d["priority"] == "HIGH"
    assert d["state"] == "pending"
    assert d["error"] is None
    assert d["retry_count"] == 0


def test_task_unique_ids():
    t1 = Task(payload={})
    t2 = Task(payload={})
    assert t1.id != t2.id


def test_task_priority_values():
    assert TaskPriority.HIGH < TaskPriority.MEDIUM
    assert TaskPriority.MEDIUM < TaskPriority.LOW
