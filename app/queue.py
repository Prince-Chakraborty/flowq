import heapq
import threading
import time
from typing import Optional
from app.models import Task, TaskState


class TaskQueue:
    def __init__(self):
        self._heap: list[Task] = []
        self._lock = threading.Lock()
        self._store: dict[str, Task] = {}

    def push(self, task: Task) -> None:
        with self._lock:
            heapq.heappush(self._heap, task)
            self._store[task.id] = task

    def pop(self) -> Optional[Task]:
        with self._lock:
            while self._heap:
                task = heapq.heappop(self._heap)
                if task.state == TaskState.PENDING:
                    task.state = TaskState.RUNNING
                    task.started_at = time.time()
                    self._store[task.id] = task
                    return task
            return None

    def get(self, task_id: str) -> Optional[Task]:
        return self._store.get(task_id)

    def update(self, task: Task) -> None:
        with self._lock:
            self._store[task.id] = task

    def size(self) -> int:
        with self._lock:
            return len(self._heap)

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for t in self._heap if t.state == TaskState.PENDING)
