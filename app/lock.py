import logging
from redis.asyncio import Redis
from app.config import settings

logger = logging.getLogger(__name__)

redis_client: Redis = None

# Lua script — atomic acquire lock
# Returns 1 if acquired, 0 if already locked
ACQUIRE_LOCK_SCRIPT = """
local key = KEYS[1]
local value = ARGV[1]
local ttl = tonumber(ARGV[2])
local result = redis.call('SET', key, value, 'NX', 'EX', ttl)
if result then
    return 1
else
    return 0
end
"""

# Lua script — atomic release lock only if we own it
RELEASE_LOCK_SCRIPT = """
local key = KEYS[1]
local value = ARGV[1]
if redis.call('GET', key) == value then
    return redis.call('DEL', key)
else
    return 0
end
"""

# Lua script — atomic visibility timeout heartbeat
HEARTBEAT_SCRIPT = """
local key = KEYS[1]
local value = ARGV[1]
local ttl = tonumber(ARGV[2])
if redis.call('GET', key) == value then
    return redis.call('EXPIRE', key, ttl)
else
    return 0
end
"""


async def init_redis():
    global redis_client
    redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    await redis_client.ping()
    logger.info("[Redis] Connected.")


async def close_redis():
    if redis_client:
        await redis_client.close()
        logger.info("[Redis] Connection closed.")


async def acquire_lock(task_id: str) -> bool:
    key = f"lock:task:{task_id}"
    result = await redis_client.eval(
        ACQUIRE_LOCK_SCRIPT, 1, key, "locked", settings.VISIBILITY_TIMEOUT
    )
    if result:
        logger.info(f"[Lock] Acquired lock for task {task_id}")
    else:
        logger.warning(f"[Lock] Task {task_id} already locked — duplicate skipped.")
    return bool(result)


async def release_lock(task_id: str) -> None:
    key = f"lock:task:{task_id}"
    result = await redis_client.eval(RELEASE_LOCK_SCRIPT, 1, key, "locked")
    if result:
        logger.info(f"[Lock] Released lock for task {task_id}")
    else:
        logger.warning(f"[Lock] Lock for task {task_id} already expired (visibility timeout triggered).")


async def heartbeat(task_id: str) -> bool:
    """Extend visibility timeout while task is still processing."""
    key = f"lock:task:{task_id}"
    result = await redis_client.eval(
        HEARTBEAT_SCRIPT, 1, key, "locked", settings.VISIBILITY_TIMEOUT
    )
    if result:
        logger.debug(f"[Lock] Heartbeat extended for task {task_id}")
    else:
        logger.warning(f"[Lock] Heartbeat failed for task {task_id} — lock expired.")
    return bool(result)


async def set_visibility_timeout(task_id: str) -> None:
    """Mark task as in-flight with visibility timeout."""
    key = f"visibility:{task_id}"
    await redis_client.set(key, "in_flight", ex=settings.VISIBILITY_TIMEOUT)
    logger.info(f"[Visibility] Task {task_id} hidden for {settings.VISIBILITY_TIMEOUT}s")


async def clear_visibility(task_id: str) -> None:
    key = f"visibility:{task_id}"
    await redis_client.delete(key)
    logger.info(f"[Visibility] Task {task_id} visibility cleared.")


async def is_task_visible(task_id: str) -> bool:
    key = f"visibility:{task_id}"
    result = await redis_client.exists(key)
    return not bool(result)
