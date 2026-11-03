import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from app.queue import TaskQueue
from app.broker import broker
from app.worker import WorkerPool
from app.routes import router
from app.database import init_db
from app.lock import init_redis, close_redis
from app.scheduler import TaskScheduler
from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def start_components(app: FastAPI):
    from app.lock import redis_client as rc
    await broker.connect()
    scheduler = TaskScheduler(redis_client=rc, broker=broker, queue_store=app.state.queue)
    await scheduler.start()
    pool = WorkerPool(broker=broker, queue_store=app.state.queue)
    await pool.start()
    app.state.scheduler = scheduler
    app.state.worker_pool = pool
    app.state.broker_ok = True
    print("[FlowQ] Phase 5 online.")


async def supervisor(app: FastAPI):
    while True:
        if not app.state.broker_ok:
            try:
                await start_components(app)
            except Exception as e:
                logger.error(f"[FlowQ] Broker unavailable, retrying in 30s: {e}")
        await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_redis()
    app.state.queue = TaskQueue()
    app.state.broker = broker
    app.state.settings = settings
    app.state.broker_ok = False
    sup = asyncio.create_task(supervisor(app))

    yield

    sup.cancel()
    await asyncio.gather(sup, return_exceptions=True)
    pool = getattr(app.state, "worker_pool", None)
    sched = getattr(app.state, "scheduler", None)
    if pool:
        await pool.stop()
    if sched:
        await sched.stop()
    try:
        await broker.close()
    except Exception:
        pass
    await close_redis()


app = FastAPI(
    title="FlowQ",
    description="Distributed Task Queue Engine",
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
        "broker": "up" if getattr(app.state, "broker_ok", False) else "down",
        "workers": settings.WORKER_COUNT,
        "max_concurrent_jobs": settings.MAX_CONCURRENT_JOBS,
        "visibility_timeout": settings.VISIBILITY_TIMEOUT,
    }
