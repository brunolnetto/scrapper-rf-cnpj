from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import pool

from .schemas import Database

# Create the database engine and session maker
def create_database_instance(uri, base):
    engine = create_engine(
        uri,
        poolclass=pool.QueuePool,   # Use connection pooling
        pool_size=20,               # Adjust pool size based on your workload
        max_overflow=10,            # Adjust maximum overflow connections
        pool_recycle=3600           # Periodically recycle connections (optional)
    )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return Database(engine=engine, session_maker=SessionLocal, base=base)
