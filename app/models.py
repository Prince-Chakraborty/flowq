from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional
import uuid
import time
from sqlalchemy import Column, String, Float, Integer, JSON
from app.database import Base


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SCHEDULED = "scheduled"


class TaskPriority(int, Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3


class TaskRecord(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True)
    payload = Column(JSON, nullable=False)
    priority = Column(String, nullable=False)
    state = Column(String, nullable=False, default="pending")
    created_at = Column(Float, nullable=False)
    scheduled_at = Column(Float, nullable=True)
    started_at = Column(Float, nullable=True)
    completed_at = Column(Float, nullable=True)
    error = Column(String, nullable=True)
    retry_count = Column(Integer, default=0)


@dataclass
class Task:
    payload: Any
    priority: TaskPriority = TaskPriority.MEDIUM
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state: TaskState = field(default=TaskState.PENDING)
    created_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    retry_count: int = 0

    def __lt__(self, other: "Task") -> bool:
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.created_at < other.created_at

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "payload": self.payload,
            "priority": self.priority.name,
            "state": self.state.value,
            "created_at": self.created_at,
            "scheduled_at": self.scheduled_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
            "retry_count": self.retry_count,
        }
