from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import pool
import asyncpg


from ..setup.logging import logger


class Database:
    """
    Represents a database connection, session management, and associated SQLAlchemy Base.
    Provides a method to create all tables for its Base.
    """

    def __init__(self, engine, session_maker, base):
        self.engine = engine
        self.session_maker = session_maker
        self.base = base

    def create_tables(self):
        """Create all tables for the associated Base in this database."""
        self.base.metadata.create_all(self.engine)
    
    async def get_async_pool(
        self
    ) -> asyncpg.Pool:
        """
        Create asyncpg connection pool from SQLAlchemy database configuration.
        
        Args:
            database: SQLAlchemy database instance
            config: Configuration service with async pool settings
            
        Returns:
            asyncpg.Pool: Ready-to-use connection pool
        """
        # Extract connection details from SQLAlchemy engine URL
        url = self.engine.url
        
        # Build asyncpg DSN from SQLAlchemy URL components
        if url.password:
            dsn = f"postgresql://{url.username}:{url.password}@{url.host}:{url.port or 5432}/{url.database}"
        else:
            dsn = f"postgresql://{url.username}@{url.host}:{url.port or 5432}/{url.database}"
        
        # Get pool configuration from loading config
        min_size = getattr(self.config.pipeline.loading, 'async_pool_min_size', 2)
        max_size = getattr(self.config.pipeline.loading, 'async_pool_max_size', 10)
        
        logger.info(f"[ConnectionFactory] Creating asyncpg pool (min: {min_size}, max: {max_size})")
        logger.debug(f"[ConnectionFactory] DSN: {dsn.split('@')[0]}@***")

        try:
            pool = await asyncpg.create_pool(
                dsn,
                min_size=min_size,
                max_size=max_size,
                command_timeout=60,
                server_settings={
                    'application_name': 'receita_cnpj_loader'
                }
            )
            logger.info("[ConnectionFactory] AsyncPG pool created successfully")
            return pool
        except Exception as e:
            logger.error(f"[ConnectionFactory] Failed to create asyncpg pool: {e}")
            raise

    
    def __repr__(self):
        return f"Database(engine={self.engine}, session_maker={self.session_maker}, base={self.base})"


# Create the database engine and session maker
def create_database_instance(uri, base):
    engine = create_engine(
        uri,
        poolclass=pool.QueuePool,  # Use connection pooling
        pool_size=20,  # Adjust pool size based on your workload
        max_overflow=10,  # Adjust maximum overflow connections
        pool_recycle=3600,  # Periodically recycle connections (optional)
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Database(engine=engine, session_maker=SessionLocal, base=base)
