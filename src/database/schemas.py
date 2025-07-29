from typing import NamedTuple

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

class TableIndex:
    """
    This class represents a table index object.
    It contains two attributes:
    - table_name: A string that represents the table name.
    - index_name: A string that represents the index name.
    """
    table_name: str
    index_name: str