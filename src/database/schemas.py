from typing import NamedTuple

class Database(NamedTuple):
  """
  This class represents a database connection and session management object.
  It contains two attributes:
  
  - engine: A callable that represents the database engine.
  - session_maker: A callable that represents the session maker.
  """
  engine: callable
  session_maker: callable
  
class TableIndex:
  """
  This class represents a table index object.
  It contains two attributes:
  
  - table_name: A string that represents the table name.
  - index_name: A string that represents the index name.
  """
  table_name: str
  index_name: str