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
    table_name: str, filenames: List[str], size_bytes: int, update_at: datetime
) -> AuditDB:
    """
    Creates a new audit entry.

    Args:
        table_name (str): The name of the table.
        filenames (List[str]): List of filenames.
        size_bytes (int): The size of the file in bytes.
        update_at (datetime): The datetime when the source was last updated.

    Returns:
        AuditDB: An audit entry object.
    """
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
    )


def create_audit(
    database: Database, file_group_info: FileGroupInfo
) -> Union[AuditDB, None]:
    """
    Inserts a new audit entry if the provided processed_at is later than the latest existing entry for the filename.

    Args:
        database (Database): The database object.
        file_group_info (FileGroupInfo): Information about the file group.

    Returns:
        Union[AuditDB, None]: The created audit entry or None if not created.
    """
    if database.engine:
        # Define raw SQL query
        sql_query = text(f"""SELECT max(audi_source_updated_at) 
            FROM public.audit 
            WHERE audi_table_name = \'{file_group_info.table_name}\';""")

        # Execute query with parameters (optional)
        with database.engine.connect() as connection:
            result = connection.execute(sql_query)

            # Process results (e.g., fetchall, fetchone)
            latest_updated_at = result.fetchone()[0]

        if latest_updated_at is not None:
            format = "%Y-%m-%d %H:%M"
            latest_updated_at = datetime.strptime(
                latest_updated_at.strftime(format), format
            )
            sao_paulo_timezone = pytz.timezone("America/Sao_Paulo")
            latest_updated_at = sao_paulo_timezone.localize(latest_updated_at)

        # First entry: no existing audit entry
        if latest_updated_at is None:
            # Create and insert the new entry
            return create_new_audit(
                file_group_info.table_name,
                file_group_info.elements,
                file_group_info.size_bytes,
                file_group_info.date_range[1],
            )

        # New entry: source updated_at is greater
        elif file_group_info.date_range[1] > latest_updated_at:
            # Create and insert the new entry
            return create_new_audit(
                file_group_info.table_name,
                file_group_info.elements,
                file_group_info.size_bytes,
                file_group_info.date_range[1],
            )

        # Not all files are updated in batch aka unreliable
        elif file_group_info.date_diff() > 7:
            return None

        else:
            summary = (
                f"Skipping create entry for file group {file_group_info.name}."
            )
            explanation = "Existing processed_at is later or equal."
            error_message = f"{summary} {explanation}"
            logger.warning(error_message)

            return None

    if latest_updated_at is None:
        # First entry: no existing audit entry
        return create_new_audit(
            file_group_info.table_name,
            file_group_info.elements,
            file_group_info.size_bytes,
            file_group_info.date_range[1],
        )
    elif file_group_info.date_range[1] > latest_updated_at:
        # New entry: source updated_at is greater
        return create_new_audit(
            file_group_info.table_name,
            file_group_info.elements,
            file_group_info.size_bytes,
            file_group_info.date_range[1],
        )
    elif file_group_info.date_diff() > 7:
        return None
    else:
        logger.warning(
            f"Skipping create entry for file group {file_group_info.name}. "
            "Existing processed_at is later or equal."
        )
        return None


def create_audits(database: Database, files_info: List[FileGroupInfo]) -> List[AuditDB]:
    """
    Creates a list of audit entries based on the provided database and files information.

    Args:
        database (Database): The database object.
        files_info (List[FileGroupInfo]): A list of file information objects.

    Returns:
        List[AuditDB]: A list of audit entries.
    """
    return [
        create_audit(database, file_info)
        for file_info in files_info
        if create_audit(database, file_info)
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

