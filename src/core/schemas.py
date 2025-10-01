from typing import NamedTuple, List, Dict, Tuple, Callable, Optional
from datetime import datetime
from pydantic import BaseModel

from ..database.models.audit import TableAuditManifestSchema

class FileInfo(BaseModel):
    """Pydantic model representing a CNPJ file."""
    filename: str
    tablename: str
    path: str
    updated_at: Optional[datetime] = None
    file_size: Optional[int] = None

class FileGroupInfo(BaseModel):
    """Pydantic model representing grouped file information."""
    table_name: str
    files: List[FileInfo]
    year: int
    month: int

    @property
    def total_size_bytes(self) -> int:
        return sum(len(file.filename.encode('utf-8')) for file in self.files)

    def __repr__(self) -> str:
        return f"FileGroupInfo(table_name='{self.table_name}', files_count={len(self.files)}, year={self.year}, month={self.month})"

class AuditMetadata(BaseModel):
    """Represents the metadata for auditing purposes."""
    audit_list: List[TableAuditManifestSchema]
    tablename_to_zipfile_to_files: Dict[str, Dict[str, List[str]]]
    
    def __repr__(self) -> str:
        args = f"audit_list={self.audit_list}, tablename_to_zipfile_to_files={self.tablename_to_zipfile_to_files}"
        return f"AuditMetadata({args})"


