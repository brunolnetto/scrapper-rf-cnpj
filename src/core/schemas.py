from typing import NamedTuple, List, Dict, Tuple, Callable
from datetime import datetime
from pydantic import BaseModel

from ..database.models import AuditDBSchema


class FileInfo(BaseModel):
    """
    Pydantic model representing a CNPJ file.
    """

    filename: str
    updated_at: datetime
    file_size: int = 0


class FileGroupInfo(BaseModel):
    """
    Pydantic model representing a group of CNPJ files.
    """

    name: str
    elements: List[str]
    date_range: Tuple[datetime, datetime]
    table_name: str
    size_bytes: int = 0

    def date_diff(self) -> float:
        """
        Returns the difference in days between the start and end dates of the group.
        """
        start, end = self.date_range
        return (end - start).days


class AuditMetadata(BaseModel):
    """
    Represents the metadata for auditing purposes.
    """

    audit_list: List[AuditDBSchema]
    tablename_to_zipfile_to_files: Dict[str, Dict[str, List[str]]]

    def __repr__(self) -> str:
        args = f"audit_list={self.audit_list}, tablename_to_zipfile_to_files={self.tablename_to_zipfile_to_files}"
        return f"AuditMetadata({args})"


class TableInfo(NamedTuple):
    """
    Represents information about a table.
    """

    label: str
    zip_group: str
    table_name: str
    columns: List[str]
    encoding: str
    transform_map: Callable
    expression: str
    table_model: object = None

