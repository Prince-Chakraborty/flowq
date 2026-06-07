import asyncio
import logging
from app.broker import broker
from app.queue import TaskQueue
from app.lock import init_redis, close_redis
from app.worker import WorkerPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


async def main():
    await init_redis()
    queue_store = TaskQueue()
    await broker.connect()

    pool = WorkerPool(broker=broker, queue_store=queue_store)
    await pool.start()

    print("[FlowQ Worker] Standalone worker node running. Press Ctrl+C to stop.")
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await pool.stop()
        await broker.close()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
