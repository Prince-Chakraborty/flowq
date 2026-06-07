from prometheus_client import Counter, Histogram, Gauge

TASKS_SUBMITTED = Counter(
    "flowq_tasks_submitted_total",
    "Total number of tasks submitted",
    ["priority"]
)

TASKS_COMPLETED = Counter(
    "flowq_tasks_completed_total",
    "Total number of tasks completed",
    ["priority"]
)

TASKS_FAILED = Counter(
    "flowq_tasks_failed_total",
    "Total number of tasks failed",
    ["priority"]
)

TASKS_RETRIED = Counter(
    "flowq_tasks_retried_total",
    "Total number of task retries",
    ["priority"]
)

TASKS_DLQ = Counter(
    "flowq_tasks_dlq_total",
    "Total number of tasks sent to DLQ",
    ["priority"]
)

TASK_PROCESSING_LATENCY = Histogram(
    "flowq_task_processing_seconds",
    "Task processing latency in seconds",
    ["priority"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

QUEUE_LENGTH = Gauge(
    "flowq_queue_length",
    "Current number of pending tasks in queue"
)

ACTIVE_WORKERS = Gauge(
    "flowq_active_workers",
    "Number of active workers"
)
