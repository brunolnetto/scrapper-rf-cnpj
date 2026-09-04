import pytest
import asyncio
from testcontainers.postgres import PostgresContainer
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from src.database.models.business import MainBase
from src.database.models.audit import AuditBase

@pytest.fixture(scope="session")
def postgres_container():
    """Start a temporary PostgreSQL container for integration tests."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def engine(postgres_container):
    """Create an async SQLAlchemy engine connected to the test container."""
    connection_url = postgres_container.get_connection_url().replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(connection_url, echo=False)
    
    async with engine.begin() as conn:
        await conn.run_sync(MainBase.metadata.create_all)
        await conn.run_sync(AuditBase.metadata.create_all)
        
    yield engine
    await engine.dispose()

@pytest.fixture
async def db_session(engine):
    """Provide a transactional database session for each test."""
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        yield session
        await session.rollback()
