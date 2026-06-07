import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from app.queue import TaskQueue
from app.broker import broker
from app.worker import WorkerPool
from app.routes import router
from app.database import init_db
from app.lock import init_redis, close_redis, redis_client
from app.scheduler import TaskScheduler
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    await init_redis()

    from app.lock import redis_client as rc
    app.state.queue = TaskQueue()
    app.state.broker = broker
    app.state.settings = settings

    await broker.connect()

    scheduler = TaskScheduler(
        redis_client=rc,
        broker=broker,
        queue_store=app.state.queue,
    )
    await scheduler.start()
    app.state.scheduler = scheduler

    worker_pool = WorkerPool(broker=broker, queue_store=app.state.queue)
    await worker_pool.start()
    app.state.worker_pool = worker_pool

    print("[FlowQ] Phase 5 online — visibility timeout, scheduler, Lua atomicity, semaphore active.")
    yield

    # Graceful shutdown
    await worker_pool.stop()
    await scheduler.stop()
    await broker.close()
    await close_redis()
    print("[FlowQ] Graceful shutdown complete.")


app = FastAPI(
    title="FlowQ",
    description="Distributed Task Queue Engine — FAANG Grade",
    version="0.5.0",
    lifespan=lifespan,
)

Instrumentator().instrument(app).expose(app)
app.include_router(router)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "0.5.0",
        "workers": settings.WORKER_COUNT,
        "max_concurrent_jobs": settings.MAX_CONCURRENT_JOBS,
        "visibility_timeout": settings.VISIBILITY_TIMEOUT,
    }
