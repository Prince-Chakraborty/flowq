from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings
import ssl

def get_engine():
    url = settings.DATABASE_URL
    if "supabase" in url:
        ssl_context = ssl.create_default_context()
        connect_args = {"ssl": ssl_context}
    else:
        connect_args = {}
    return create_async_engine(url, echo=False, connect_args=connect_args)

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
