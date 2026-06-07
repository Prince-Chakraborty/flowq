from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings


def get_engine():
    url = settings.DATABASE_URL
    # Replace postgresql:// with postgresql+asyncpg://
    url = url.replace("postgresql://", "postgresql+asyncpg://")
    # Remove channel_binding parameter — not supported by asyncpg
    url = url.replace("&channel_binding=require", "")
    url = url.replace("?channel_binding=require&", "?")
    return create_async_engine(url, echo=False)


engine = get_engine()
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
