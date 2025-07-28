from typing import Dict, List, Optional, Any
import re
from urllib import request
from bs4 import BeautifulSoup
from datetime import datetime
from functools import reduce
from os import getenv
import pytz

from setup.logging import logger
from core.utils.schemas import create_file_groups
from database.models import AuditDB
from core.schemas import FileInfo, AuditMetadata
from database.utils.models import (
    create_audits, 
    create_audit_metadata, 
    insert_audit
)
from database.dml import generate_tables_indices
from core.utils.etl import (
    get_RF_data, 
    load_RF_data_on_database
)
from utils.misc import convert_to_bytes, remove_folder

class CNPJ_ETL:

    def __init__(
        self, database, data_url, layout_url, download_folder, extract_folder,
        is_parallel=True, delete_zips=True
    ) -> None:
        self.database = database
        self.data_url = data_url
        self.layout_url = layout_url
        self.download_folder = download_folder
        self.extract_folder = extract_folder
        self.is_parallel = is_parallel
        self.delete_zips = delete_zips
        
    def scrap_data(self) -> List[FileInfo]:
        """
        Scrapes the Receita Federal website to extract file information.
        
        Returns:
            List[FileInfo]: A list of FileInfo objects with updated date, filename, and size.
        """
        try:
            raw_html = request.urlopen(self.data_url).read()
        except Exception as e:
            logger.error(f"Failed to fetch URL: {self.data_url}, error: {e}")
            return []

        page_items = BeautifulSoup(raw_html, 'lxml')
        table_rows = page_items.find_all('tr')

        files_info = []
        for row in table_rows:
            filename_cell = row.find('a')
            date_cell = row.find('td', text=lambda text: text and re.search(r'\d{4}-\d{2}-\d{2}', text))
            size_cell = row.find('td', text=lambda text: text and any(text.endswith(size_type) for size_type in ['K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']))

            if filename_cell and date_cell and size_cell:
                filename = filename_cell.text.strip()
                if filename.endswith('.zip'):
                    updated_at = self._parse_date(date_cell.text.strip(), filename)
                    if not updated_at:
                        continue

                    file_info = FileInfo(
                        filename=filename,
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_cell.text.strip())
                    )
                    files_info.append(file_info)

        return files_info

    @staticmethod
    def _parse_date(date_str: str, filename: str) -> Optional[datetime]:
        """
        Parses date from string, logs errors if date parsing fails.
        
        Args:
            date_str (str): The date string to parse.
            filename (str): The file being processed (for error logging).
        
        Returns:
            Optional[datetime]: Parsed datetime object or None if parsing fails.
        """
        try:
            updated_at = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return pytz.timezone("America/Sao_Paulo")\
                .localize(updated_at)\
                .replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            logger.error(f"Error parsing date for file: {filename}")
            return None    
    
    def get_data(self, audits: List[AuditDB]) -> None:
        """
        Downloads the files from the Receita Federal website.
        
        Args:
            audits (List[AuditDB]): List of audit objects to download data for.
        """

        if audits:
            get_RF_data(
                self.data_url, self.layout_url, 
                audits, self.download_folder, self.extract_folder, 
                self.is_parallel
            )

    def audit_scrapped_files(self, files_info: List[FileInfo]) -> List[AuditDB]:
        """
        Audits the scrapped files.
        
        Args:
            files_info (List[FileInfo]): List of file information.
        
        Returns:
            List[AuditDB]: List of audited file objects.
        """
        filtered_files = [
            info for info in files_info if info.filename.endswith('.zip') or
            (info.filename.endswith('.pdf') and re.match(r'layout', info.filename, re.IGNORECASE))
        ]

        file_groups_info = create_file_groups(filtered_files)
        
        return create_audits(self.database, file_groups_info)

    def fetch_data(self) -> List[AuditDB]:
        """
        Fetches and audits scrapped data from the Receita Federal site.
        
        Returns:
            List[AuditDB]: List of audit objects.
        """
        files_info = self.scrap_data()

        return self.audit_scrapped_files(files_info)

    def retrieve_data(self) -> List[AuditDB]:
        """
        Retrieves audit data and filters based on environment if in development mode.
        
        Returns:
            List[AuditDB]: Filtered audit objects.
        """
        audits = self.fetch_data()

        if getenv("ENVIRONMENT") == "development":
            audits = [
                audit for audit in sorted(audits, key=lambda x: x.audi_file_size_bytes)
                if audit.audi_file_size_bytes < 50000
            ]

        if audits:
            self.get_data(audits)
        else:
            logger.warning("No audits to retrieve.")
        return audits

    def load_data(self, audit_metadata: AuditMetadata) -> None:
        """
        Loads data into the database.
        
        Args:
            audit_metadata (AuditMetadata): Metadata for the audit.
        """
        load_RF_data_on_database(self.database, self.extract_folder, audit_metadata)

    def insert_audits(self, audit_metadata: AuditMetadata) -> None:
        """
        Inserts audit metadata into the database.

        Args:
            audit_metadata (AuditMetadata): Metadata for the audit.
        """
        for audit in audit_metadata.audit_list:
            try:
                insert_audit(self.database, audit.to_audit_db())
            except Exception as e:
                logger.error(f"Error inserting audit {audit}: {e}")

    def load_without_download(self) -> None:
        """
        Loads data directly into the database without downloading.
        """
        audits = self.fetch_data()
        if audits:
            audit_metadata = self._prepare_audit_metadata(audits)
            self._process_and_load(audit_metadata)
        else:
            logger.warning("No data to load!")

<<<<<<< HEAD
    def _prepare_audit_metadata(self, audits: List[AuditDB]) -> AuditMetadata:
        audit_metadata = create_audit_metadata(audits, self.download_folder)
        for audit in audit_metadata.audit_list:
            audit.audi_downloaded_at = audit.audi_processed_at = datetime.now()
        return audit_metadata
=======
            # Create indices
            self.create_indices(audit_metadata)
>>>>>>> ace4e8e (refactor: review names and fix methods)

    def _process_and_load(self, audit_metadata: AuditMetadata) -> None:
        self.load_data(audit_metadata)
        self.create_indices()
        self.insert_audits(audit_metadata)

    def only_create_indices(self) -> None:
        """
        Generates and inserts table indices.
        """
        self.create_indices()

    def create_indices(self) -> None:
        """
        Generates indices for specific tables based on audit metadata.
        
        Args:
            audit_metadata (AuditMetadata): Metadata for the audit.
        """
        tables_with_indices = {
            "estabelecimento": {
                "cnpj_basico", "cnpj_ordem", "cnpj_dv", 
                "cnae_fiscal_principal", "cnae_fiscal_secundaria", 
                "cep", "municipio", "uf"
            },
            "empresa": {"cnpj_basico"},
            "simples": {"cnpj_basico"}, 
            "socios": {"cnpj_basico"},
            "cnae": {"codigo"},
            "moti": {"codigo"},
            "munic": {"codigo"}
        }

        renew_table_indices = {
            table_name: columns 
            for table_name, columns in tables_with_indices.items() 
        }

        if renew_table_indices:
            generate_tables_indices(self.database.engine, renew_table_indices)

    def run(self) -> None:
        """
        Runs the full ETL process: retrieves, audits, loads, and inserts data.
        """
        audits = self.retrieve_data()

        if audits:
            audit_metadata = self._prepare_audit_metadata(audits)
            self._process_and_load(audit_metadata)

            if self.delete_zips:
                remove_folder(self.download_folder)
        else:
            logger.warning("No data to load!")
