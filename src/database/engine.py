from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import pool


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
