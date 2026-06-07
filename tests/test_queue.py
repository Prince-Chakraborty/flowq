import pytest
from app.queue import TaskQueue
from app.models import Task, TaskPriority, TaskState


def test_push_and_get():
    q = TaskQueue()
    task = Task(payload={"job": "test"})
    q.push(task)
    assert q.get(task.id) is not None


def test_pop_returns_highest_priority():
    q = TaskQueue()
    low = Task(payload={}, priority=TaskPriority.LOW)
    high = Task(payload={}, priority=TaskPriority.HIGH)
    medium = Task(payload={}, priority=TaskPriority.MEDIUM)
    q.push(low)
    q.push(medium)
    q.push(high)
    popped = q.pop()
    assert popped.priority == TaskPriority.HIGH


def test_pop_sets_running_state():
    q = TaskQueue()
    task = Task(payload={})
    q.push(task)
    popped = q.pop()
    assert popped.state == TaskState.RUNNING


def test_pop_empty_queue():
    q = TaskQueue()
    assert q.pop() is None


def test_queue_size():
    q = TaskQueue()
    q.push(Task(payload={}))
    q.push(Task(payload={}))
    assert q.size() == 2


def test_pending_count():
    q = TaskQueue()
    q.push(Task(payload={}))
    q.push(Task(payload={}))
    assert q.pending_count() == 2


def test_update_task():
    q = TaskQueue()
    task = Task(payload={"job": "test"})
    q.push(task)
    task.state = TaskState.COMPLETED
    q.update(task)
    assert q.get(task.id).state == TaskState.COMPLETED
