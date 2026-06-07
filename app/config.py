from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # RabbitMQ
    RABBITMQ_URL: str = "amqp://guest:guest@localhost/"
    QUEUE_NAME: str = "flowq.tasks"
    DLQ_NAME: str = "flowq.tasks.dlq"

    # Worker
    WORKER_COUNT: int = 3
    MAX_CONCURRENT_JOBS: int = 10
    TASK_TIMEOUT: int = 30

    # Visibility timeout (seconds) — task reappears if worker crashes
    VISIBILITY_TIMEOUT: int = 60

    # PostgreSQL
    DATABASE_URL: str = "postgresql+asyncpg://princechakraborty@localhost/flowq"

    # Redis
    REDIS_URL: str = "redis://localhost:6379"
    LOCK_TTL: int = 60

    class Config:
        env_file = ".env"


settings = Settings()
