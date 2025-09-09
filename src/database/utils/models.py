from datetime import datetime
from typing import Union, List
from os import path
from uuid import uuid4

from sqlalchemy import text
import pytz

from ...setup.logging import logger
from ...utils.misc import invert_dict_list
from ...utils.zip import list_zip_contents
from ...core.utils.etl import get_zip_to_tablename
from ..engine import Database
from ..models import AuditDB
from ...core.schemas import FileGroupInfo, AuditMetadata, AuditDBSchema


def create_new_audit(
    table_name: str, filenames: List[str], size_bytes: int, update_at: datetime,
    year: int = None, month: int = None
) -> AuditDB:
    """
    Creates a new audit entry with temporal tracking fields.

    Args:
        table_name (str): The name of the table.
        filenames (List[str]): List of filenames.
        size_bytes (int): The size of the file in bytes.
        update_at (datetime): The datetime when the source was last updated.
        year (int, optional): Year for temporal tracking. Defaults to current year.
        month (int, optional): Month for temporal tracking. Defaults to current month.

    Returns:
        AuditDB: An audit entry object with temporal fields populated.
    """
    # Set default temporal values if not provided
    current_year = year if year is not None else datetime.now().year
    current_month = month if month is not None else datetime.now().month

    return AuditDB(
        audi_id=str(uuid4()),
        audi_created_at=datetime.now(),
        audi_table_name=table_name,
        audi_filenames=filenames,
        audi_file_size_bytes=size_bytes,
        audi_source_updated_at=update_at,
        audi_downloaded_at=None,
        audi_processed_at=None,
        audi_inserted_at=None,
        audi_ingestion_year=current_year,    # FIXED: Add temporal year
        audi_ingestion_month=current_month   # FIXED: Add temporal month
    )


def create_audit(
    database: Database, file_group_info: FileGroupInfo,
    etl_year: int = None, etl_month: int = None
) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        database (Database): The database object.
        file_group_info (FileGroupInfo): Information about the file group.
        etl_year (int, optional): ETL processing year. If provided, overrides source year.
        etl_month (int, optional): ETL processing month. If provided, overrides source month.

    Returns:
        Union[AuditDB, None]: The created audit entry or None if not created.
    """
    if database.engine:
        # Define temporal-aware SQL query
        if etl_year is not None and etl_month is not None:
            # Check for existing processing in the specific ETL year/month context
            sql_query = text(f"""SELECT max(audi_source_updated_at) 
                FROM public.table_ingestion_manifest 
                WHERE audi_table_name = :table_name 
                AND audi_ingestion_year = :etl_year 
                AND audi_ingestion_month = :etl_month;""")
            
            # Execute query with ETL temporal parameters
            with database.engine.connect() as connection:
                result = connection.execute(sql_query, {
                    'table_name': file_group_info.table_name,
                    'etl_year': etl_year,
                    'etl_month': etl_month
                })
                latest_updated_at = result.fetchone()[0]
        else:
            # Legacy behavior: check without temporal filtering
            sql_query = text(f"""SELECT max(audi_source_updated_at) 
                FROM public.table_ingestion_manifest 
                WHERE audi_table_name = :table_name;""")
            
            # Execute query with parameters
            with database.engine.connect() as connection:
                result = connection.execute(sql_query, {
                    'table_name': file_group_info.table_name
                })
                latest_updated_at = result.fetchone()[0]

        if latest_updated_at is not None:
            format = "%Y-%m-%d %H:%M"
            latest_updated_at = datetime.strptime(
                latest_updated_at.strftime(format), format
            )
            sao_paulo_timezone = pytz.timezone("America/Sao_Paulo")
            latest_updated_at = sao_paulo_timezone.localize(latest_updated_at)

        # Extract temporal info - use ETL config if provided, otherwise use source date
        if etl_year is not None and etl_month is not None:
            # Use ETL configuration for temporal tracking
            data_year = etl_year
            data_month = etl_month
        else:
            # Use source date for temporal tracking (legacy behavior)
            source_date = file_group_info.date_range[1]
            data_year = source_date.year
            data_month = source_date.month

        # First entry: no existing audit entry
        if latest_updated_at is None:
            # Create and insert the new entry
            return create_new_audit(
                file_group_info.table_name,
                file_group_info.elements,
                file_group_info.size_bytes,
                file_group_info.date_range[1],
                data_year,  # Use ETL year or source year
                data_month  # Use ETL month or source month
            )

        # New entry: source updated_at is greater
        elif file_group_info.date_range[1] > latest_updated_at:
            # Create and insert the new entry
            return create_new_audit(
                file_group_info.table_name,
                file_group_info.elements,
                file_group_info.size_bytes,
                file_group_info.date_range[1],
                data_year,  # Use ETL year or source year
                data_month  # Use ETL month or source month
            )

        else:
            summary = (
                f"Skipping create entry for file group {file_group_info.name}."
            )
            explanation = "Existing processed_at is later or equal."
            error_message = f"{summary} {explanation}"
            logger.warning(error_message)

            return None

    # Extract temporal info - use ETL config if provided, otherwise use source date
    if etl_year is not None and etl_month is not None:
        # Use ETL configuration for temporal tracking
        data_year = etl_year
        data_month = etl_month
    else:
        # Use source date for temporal tracking (legacy behavior)
        source_date = file_group_info.date_range[1]
        data_year = source_date.year
        data_month = source_date.month

    if latest_updated_at is None:
        # First entry: no existing audit entry
        return create_new_audit(
            file_group_info.table_name,
            file_group_info.elements,
            file_group_info.size_bytes,
            file_group_info.date_range[1],
            data_year,   # Use ETL year or source year
            data_month   # Use ETL month or source month
        )
    elif file_group_info.date_range[1] > latest_updated_at:
        # New entry: source updated_at is greater
        return create_new_audit(
            file_group_info.table_name,
            file_group_info.elements,
            file_group_info.size_bytes,
            file_group_info.date_range[1],
            data_year,   # Use ETL year or source year
            data_month   # Use ETL month or source month
        )
    else:
        logger.warning(
            f"Skipping create entry for file group {file_group_info.name}. "
            "Existing processed_at is later or equal."
        )
        return None


def create_audits(database: Database, files_info: List[FileGroupInfo], 
                 etl_year: int = None, etl_month: int = None) -> List[AuditDB]:
    """
    Creates a list of audit entries based on the provided database and files information.

    Args:
        database (Database): The database object.
        files_info (List[FileGroupInfo]): A list of file information objects.
        etl_year (int, optional): ETL processing year. Defaults to None (uses source year).
        etl_month (int, optional): ETL processing month. Defaults to None (uses source month).

    Returns:
        List[AuditDB]: A list of audit entries.
    """
    return [
        create_audit(database, file_info, etl_year, etl_month)
        for file_info in files_info
        if create_audit(database, file_info, etl_year, etl_month)
    ]


def insert_audit(database: Database, new_audit: AuditDB) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        database (Database): The database object.
        new_audit (AuditDB): The new audit entry.

    Returns:
        Union[AuditDB, None]: The inserted audit entry or None if not inserted.
    """
    if database.engine:
        with database.session_maker() as session:
            if new_audit.is_precedence_met:
                session.add(new_audit)
                session.commit()
                return new_audit
            else:
                logger.warning(
                    f"Skipping insert audit for table name {new_audit.audi_table_name}."
                )
                return None
    else:
        logger.error("Error connecting to the database!")
        return None


def insert_audits(database: Database, new_audits: List[AuditDB]) -> None:
    """
    Inserts a list of new audits into the database.

    Args:
        database (Database): The database object.
        new_audits (List[AuditDB]): A list of new audit entries.

    Returns:
        None
    """
    for new_audit in new_audits:
        try:
            insert_audit(database, new_audit)
        except Exception as e:
            logger.error(
                f"Error inserting audit for table {new_audit.audi_table_name}: {e}"
            )


def create_new_audit_metadata(audits: List[AuditDB]) -> AuditMetadata:
    """
    Creates audit metadata based on the provided database, files information, and destination path.

    Args:
        audits (List[AuditDB]): A list of audit entries.
        to_path (str): The destination path for the files.

    Returns:
        AuditMetadata: An object containing the audit list and related metadata.
    """
    zip_file_dict = {
        zip_filename: [] for audit in audits for zip_filename in audit.audi_filenames
    }

    zipfiles_to_tablenames = get_zip_to_tablename(zip_file_dict)
    tablename_to_zipfile_dict = invert_dict_list(zipfiles_to_tablenames)

    tablename_to_zipfile_to_files = {
        tablename: {zipfile: zip_file_dict[zipfile] for zipfile in zipfiles}
        for tablename, zipfiles in tablename_to_zipfile_dict.items()
    }
    return AuditMetadata(
        audit_list=[
            AuditDBSchema.model_validate(audit, from_attributes=True)
            for audit in audits
        ],
        tablename_to_zipfile_to_files=tablename_to_zipfile_to_files,
    )


def create_audit_metadata(audits: List[AuditDB], to_path: str) -> AuditMetadata:
    """
    Creates audit metadata based on the provided database, files information, and destination path.

    Args:
        audits (List[AuditDB]): A list of audit entries.
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
        for zip_filename in audit.audi_filenames
    }

    zipfiles_to_tablenames = get_zip_to_tablename(zip_file_dict)

    tablename_to_zipfile_dict = invert_dict_list(zipfiles_to_tablenames)

    tablename_to_zipfile_to_files = {
        tablename: {zipfile: zip_file_dict[zipfile] for zipfile in zipfiles}
        for tablename, zipfiles in tablename_to_zipfile_dict.items()
    }
    return AuditMetadata(
        audit_list=[
            AuditDBSchema.model_validate(audit, from_attributes=True)
            for audit in audits
        ],
        tablename_to_zipfile_to_files=tablename_to_zipfile_to_files,
    )

