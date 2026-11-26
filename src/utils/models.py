from datetime import datetime
from typing import Union, List
from os import path
from uuid import uuid4

from sqlalchemy import text

from .misc import invert_dict_list
from .zip import list_zip_contents
from ..database.engine import Database
from ..database.models.audit import TableAuditManifest, AuditStatus
from ..setup.logging import logger
from ..core.utils.pipeline import get_zip_to_tablename
from ..core.schemas import FileGroupInfo, AuditMetadata, TableAuditManifestSchema


def create_new_audit(
    table_name: str, filenames: List[str],
    year: int = None, month: int = None
) -> TableAuditManifest:
    """
    Creates a new audit entry with temporal tracking fields.

    Args:
        table_name (str): The name of the table.
        filenames (List[str]): List of filenames.
        year (int, optional): Year for temporal tracking. Defaults to current year.
        month (int, optional): Month for temporal tracking. Defaults to current month.

    Returns:
        TableAuditManifest: An audit entry object with temporal fields populated.
    """
    # Set default temporal values if not provided
    current_year = year if year is not None else datetime.now().year
    current_month = month if month is not None else datetime.now().month

    return TableAuditManifest(
        table_audit_id=str(uuid4()),  # Updated column name
        created_at=datetime.now(),
        entity_name=table_name,  # Updated column name
        source_files=filenames,
        status=AuditStatus.PENDING,
        notes=None,
        ingestion_year=current_year,
        ingestion_month=current_month
    )


def _audit_period(file_group_info: FileGroupInfo, etl_year: int = None, etl_month: int = None) -> tuple[int, int]:
    """Period recorded on the audit: the ETL period when given, else the source date."""
    if etl_year is not None and etl_month is not None:
        return etl_year, etl_month
    latest_update = max((f.updated_at for f in file_group_info.files if f.updated_at), default=None)
    if latest_update:
        return latest_update.year, latest_update.month
    now = datetime.now()
    return now.year, now.month


def _has_completed_audit(database: Database, table_name: str, etl_year: int = None, etl_month: int = None) -> bool:
    """Whether the table already has a COMPLETED audit (for the ETL period, when given).

    Only completed audits count: FAILED, PENDING or RUNNING audits left by a
    failed or interrupted run must not stop the table from being retried.
    """
    params = {"table_name": table_name, "completed": AuditStatus.COMPLETED.value}
    period_filter = ""
    if etl_year is not None and etl_month is not None:
        period_filter = " AND ingestion_year = :etl_year AND ingestion_month = :etl_month"
        params.update(etl_year=etl_year, etl_month=etl_month)
    sql_query = text(
        "SELECT COUNT(*) FROM public.table_audit_manifest "
        "WHERE entity_name = :table_name AND status = :completed" + period_filter
    )
    with database.engine.connect() as connection:
        return connection.execute(sql_query, params).scalar() > 0


def create_audit(
    database: Database, file_group_info: FileGroupInfo,
    etl_year: int = None, etl_month: int = None
) -> Union[TableAuditManifest, None]:
    """
    Build a new (not yet inserted) audit entry for a file group, unless the
    table was already successfully processed.

    Args:
        database (Database): The database object.
        file_group_info (FileGroupInfo): Information about the file group.
        etl_year (int, optional): ETL processing year. If provided, overrides source year.
        etl_month (int, optional): ETL processing month. If provided, overrides source month.

    Returns:
        Union[TableAuditManifest, None]: The audit entry, or None when the table
        already has a COMPLETED audit for the period (or the database is unavailable).
    """
    if not database.engine:
        logger.error("Error connecting to the database!")
        return None

    table_name = file_group_info.table_name
    if _has_completed_audit(database, table_name, etl_year, etl_month):
        logger.warning(
            f"Skipping create entry for file group {table_name}. "
            "Table already processed successfully for this year/month."
        )
        return None

    data_year, data_month = _audit_period(file_group_info, etl_year, etl_month)
    return create_new_audit(
        table_name,
        [f.filename for f in file_group_info.files],
        data_year,
        data_month,
    )


def create_audits(database: Database, files_info: List[FileGroupInfo],
                 etl_year: int = None, etl_month: int = None) -> List[TableAuditManifest]:
    """
    Creates a list of audit entries based on the provided database and files information.

    Args:
        database (Database): The database object.
        files_info (List[FileGroupInfo]): A list of file information objects.
        etl_year (int, optional): ETL processing year. Defaults to None (uses source year).
        etl_month (int, optional): ETL processing month. Defaults to None (uses source month).

    Returns:
        List[TableAuditManifest]: A list of audit entries.
    """
    audits = (create_audit(database, file_info, etl_year, etl_month) for file_info in files_info)
    return [audit for audit in audits if audit is not None]


def insert_audit(database: Database, new_audit: TableAuditManifest) -> Union[TableAuditManifest, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        database (Database): The database object.
        new_audit (TableAuditManifest): The new audit entry.

    Returns:
        Union[TableAuditManifest, None]: The inserted audit entry or None if not inserted.
    """
    if database.engine:
        with database.session_maker() as session:
            if new_audit.is_precedence_met:
                session.add(new_audit)
                session.commit()
                return new_audit
            else:
                logger.warning(
                    f"Skipping insert audit for table name {new_audit.entity_name}."
                )
                return None
    else:
        logger.error("Error connecting to the database!")
        return None


def insert_audits(database: Database, new_audits: List[TableAuditManifest]) -> None:
    """
    Inserts a list of new audits into the database.

    Args:
        database (Database): The database object.
        new_audits (List[TableAuditManifest]): A list of new audit entries.

    Returns:
        None
    """
    for new_audit in new_audits:
        try:
            insert_audit(database, new_audit)
        except Exception as e:
            logger.error(
                f"Error inserting audit for table {new_audit.entity_name}: {e}"
            )


def create_new_audit_metadata(audits: List[TableAuditManifest]) -> AuditMetadata:
    """
    Creates audit metadata based on the provided database, files information, and destination path.

    Args:
        audits (List[TableAuditManifest]): A list of audit entries.
        to_path (str): The destination path for the files.

    Returns:
        AuditMetadata: An object containing the audit list and related metadata.
    """
    zip_file_dict = {
        zip_filename: [] for audit in audits for zip_filename in audit.source_files
    }

    zipfiles_to_tablenames = get_zip_to_tablename(zip_file_dict)
    tablename_to_zipfile_dict = invert_dict_list(zipfiles_to_tablenames)

    tablename_to_zipfile_to_files = {
        tablename: {zipfile: zip_file_dict[zipfile] for zipfile in zipfiles}
        for tablename, zipfiles in tablename_to_zipfile_dict.items()
    }
    return AuditMetadata(
        audit_list=[
            TableAuditManifestSchema.model_validate(audit, from_attributes=True)
            for audit in audits
        ],
        tablename_to_zipfile_to_files=tablename_to_zipfile_to_files,
    )


def create_audit_metadata(audits: List[TableAuditManifest], to_path: str) -> AuditMetadata:
    """
    Creates audit metadata based on the provided database, files information, and destination path.

    Args:
        audits (List[TableAuditManifest]): A list of audit entries.
        to_path (str): The destination path for the files.

    Returns:
        AuditMetadata: An object containing the audit list and related metadata.
    """
    zip_file_dict = {
        zip_filename: [
            content.filename
            for content in list_zip_contents(path.join(to_path, zip_filename))
        ]
        for audit in audits
        for zip_filename in audit.source_files
    }

    zipfiles_to_tablenames = get_zip_to_tablename(zip_file_dict)

    tablename_to_zipfile_dict = invert_dict_list(zipfiles_to_tablenames)

    tablename_to_zipfile_to_files = {
        tablename: {zipfile: zip_file_dict[zipfile] for zipfile in zipfiles}
        for tablename, zipfiles in tablename_to_zipfile_dict.items()
    }
    return AuditMetadata(
        audit_list=[
            TableAuditManifestSchema.model_validate(audit, from_attributes=True)
            for audit in audits
        ],
        tablename_to_zipfile_to_files=tablename_to_zipfile_to_files,
    )

