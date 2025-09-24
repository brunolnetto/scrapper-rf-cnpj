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
from ..core.utils.etl import get_zip_to_tablename
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
        # Removed file_size_bytes and source_updated_at - now tracked at file level
        status=AuditStatus.PENDING,
        notes=None,
        ingestion_year=current_year,
        ingestion_month=current_month
    )


def create_audit(
    database: Database, file_group_info: FileGroupInfo,
    etl_year: int = None, etl_month: int = None
) -> Union[TableAuditManifest, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        database (Database): The database object.
        file_group_info (FileGroupInfo): Information about the file group.
        etl_year (int, optional): ETL processing year. If provided, overrides source year.
        etl_month (int, optional): ETL processing month. If provided, overrides source month.

    Returns:
        Union[TableAuditManifest, None]: The created audit entry or None if not created.
    """
    if database.engine:
        # Define temporal-aware SQL query
        if etl_year is not None and etl_month is not None:
            # Check for existing processing in the specific ETL year/month context
            sql_query = text("""SELECT COUNT(*) 
                FROM public.table_audit_manifest 
                WHERE entity_name = :table_name 
                AND ingestion_year = :etl_year 
                AND ingestion_month = :etl_month;""")
            
            # Execute query with ETL temporal parameters
            with database.engine.connect() as connection:
                result = connection.execute(sql_query, {
                    'table_name': file_group_info.table_name,
                    'etl_year': etl_year,
                    'etl_month': etl_month
                })
                existing_count = result.fetchone()[0]
                already_processed = existing_count > 0
        else:
            # Legacy behavior: check without temporal filtering
            sql_query = text("""SELECT COUNT(*) 
                FROM public.table_audit_manifest 
                WHERE entity_name = :table_name;""")
            
            # Execute query with parameters
            with database.engine.connect() as connection:
                result = connection.execute(sql_query, {
                    'table_name': file_group_info.table_name
                })
                existing_count = result.fetchone()[0]
                already_processed = existing_count > 0

        # Extract temporal info - use ETL config if provided, otherwise use source date
        if etl_year is not None and etl_month is not None:
            # Use ETL configuration for temporal tracking
            data_year = etl_year
            data_month = etl_month
        else:
            # Use source date for temporal tracking (legacy behavior)
            # For new FileGroupInfo, get the latest update time from files
            latest_update = max((f.updated_at for f in file_group_info.files if f.updated_at), default=None)
            if latest_update:
                data_year = latest_update.year
                data_month = latest_update.month
            else:
                data_year = datetime.now().year
                data_month = datetime.now().month

        if already_processed:
            summary = (
                f"Skipping create entry for file group {file_group_info.table_name}."
            )
            explanation = "Table already processed for this year/month."
            error_message = f"{summary} {explanation}"
            logger.warning(error_message)
            return None
        else:
            # Create and insert the new entry
            from ..database.models.audit import TableAuditManifest
            
            audit = TableAuditManifest(
                entity_name=file_group_info.table_name,
                status=AuditStatus.PENDING,
                source_files=[f.filename for f in file_group_info.files],
                # Removed file_size_bytes - now tracked at file level
                ingestion_year=data_year,
                ingestion_month=data_month,
            )
            return audit

    # Extract temporal info - use ETL config if provided, otherwise use source date
    if etl_year is not None and etl_month is not None:
        # Use ETL configuration for temporal tracking
        data_year = etl_year
        data_month = etl_month
    else:
        # Use source date for temporal tracking (legacy behavior)
        # For new FileGroupInfo, get the latest update time from files
        latest_update = max((f.updated_at for f in file_group_info.files if f.updated_at), default=None)
        if latest_update:
            data_year = latest_update.year
            data_month = latest_update.month
        else:
            data_year = datetime.now().year
            data_month = datetime.now().month

    if already_processed:
        summary = (
            f"Skipping create entry for file group {file_group_info.table_name}."
        )
        explanation = "Table already processed for this year/month."
        error_message = f"{summary} {explanation}"
        logger.warning(error_message)
        return None
    else:
        # Create and insert the new entry
        return create_new_audit(
            file_group_info.table_name,
            [f.filename for f in file_group_info.files],  # Extract filenames
            data_year,  # Use ETL year or source year
            data_month  # Use ETL month or source month
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
    return [
        create_audit(database, file_info, etl_year, etl_month)
        for file_info in files_info
        if create_audit(database, file_info, etl_year, etl_month)
    ]


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

