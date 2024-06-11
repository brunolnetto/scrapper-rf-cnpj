from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import pool

from .schemas import Database

# Default session timeout: 5 hours
DEFAULT_SESSION_TIMEOUT = 5*60*60

# Create the database engine and session maker
def create_database(uri, session_timeout: int = DEFAULT_SESSION_TIMEOUT):
    engine = create_engine(
        uri,
        poolclass=pool.QueuePool,   # Use connection pooling
        pool_size=20,               # Adjust pool size based on your workload
        max_overflow=10,            # Adjust maximum overflow connections
        pool_recycle=3600           # Periodically recycle connections (optional)
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return Database(engine=engine, session_maker=SessionLocal)