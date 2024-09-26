from typing import List
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
        Scrapes the RF (Receita Federal) website to extract file information.

        Returns:
            list: A list of FileInfo objects containing the updated date and filename of the files found on the RF website.
        """
        raw_html = request.urlopen(self.data_url).read()
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
                    try:
                        updated_at = datetime.strptime(date_cell.text.strip(), "%Y-%m-%d %H:%M")
                        updated_at = pytz.timezone("America/Sao_Paulo").localize(updated_at).replace(hour=0, minute=0, second=0, microsecond=0)
                    except ValueError:
                        logger.error(f"Error parsing date for file: {filename}")
                        continue

                    file_info = FileInfo(
                        filename=filename, 
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_cell.text.strip())
                    )
                    files_info.append(file_info)
        
        return files_info
    
    def get_data(self, audits: List[AuditDB]) -> None:
        """
        Downloads the files from the RF (Receita Federal) website.

        Args:
            audits (List[AuditDB]): List of audit objects.
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
            List[AuditDB]: List of audit objects.
        """
        files_info = [info for info in files_info if info.filename.endswith('.zip') or (info.filename.endswith('.pdf') and re.match(r'layout', info.filename, re.IGNORECASE))]
        file_groups_info = create_file_groups(files_info)
        return create_audits(self.database, file_groups_info)

        # Create audits
        audits = create_audits(self.database, file_groups_info)

        return audits

    def fetch_data(self):
        """
        Filters the data to be loaded into the database.

        Returns:
            List[AuditDB]: List of audit objects.
        """
        files_info = self.scrap_data()
        return self.audit_scrapped_files(files_info)

        # Audit scrapped files
        audits = self.audit_scrapped_files(files_info)

        return audits

    def retrieve_data(self):
        """
        Retrieves the data from the database.

        Returns:
            List[AuditDB]: List of audit objects.
        """
        audits = self.fetch_data()
        
        if getenv("ENVIRONMENT") == "development": 
            audits = list(
                filter(
                    lambda x: x.audi_file_size_bytes < 50000, 
                    sorted(audits, key=lambda x: x.audi_file_size_bytes)
                )
            )
        
        if audits:
            self.get_data(audits)
            return audits
        return []

    def load_data(self, audit_metadata: AuditMetadata) -> None:
        """
        Loads the data into the database.

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
                insert_audit(self.database, audit)
            except Exception as e:
                logger.error(f"Error inserting audit {audit}: {e}")

    def load_without_download(self) -> None:
        """
        Uploads the data to the database without downloading it.
        """
        audits = self.fetch_data()

        if audits:
            audit_metadata = create_audit_metadata(audits, self.download_folder)
            for audit in audit_metadata.audit_list:
                audit.audi_downloaded_at=datetime.now()
                audit.audi_processed_at=datetime.now()

            # Load data
            self.load_data(audit_metadata)

            # Create indices
            self.create_indices(audit_metadata)

            # Insert audit metadata
            self.insert_audits(audit_metadata)
        else: 
            logger.warn("No data to load!")

    def only_create_indices(self):
        audits = self.fetch_data()

        if audits:
            audit_metadata = create_audit_metadata(audits, self.download_folder)
            for audit in audit_metadata.audit_list:
                audit.audi_downloaded_at = datetime.now()
                audit.audi_processed_at = datetime.now()
                audit.audi_inserted_at = datetime.now()
            
            self.create_indices(audit_metadata)
            self.insert_audits(audit_metadata)
        else: 
            logger.warn("No data to load!")
        
    def create_indices(self, audit_metadata: AuditMetadata) -> None:
        """
        Generates indices for the tables.

        Args:
            audit_metadata (AuditMetadata): Metadata for the audit.
        """
        table_to_filenames = audit_metadata.tablename_to_zipfile_to_files
        zip_tablenames_set = set(table_to_filenames.keys())

        tables_with_indices = {
            "estabelecimento": {
                "cnpj_basico", 
                "cnpj_ordem", 
                "cnpj_dv", 
                "cnae_principal", 
                "cnae_secundaria", 
                "cep", 
                "municipio",
                "uf"
            },
            "empresa": { "cnpj_basico" },
            "simples": { "cnpj_basico" }, 
            "socios": { "cnpj_basico" },
            "cnae": { "codigo" },
            "moti": { "codigo" },
            "munic": { "codigo" }
        }
        tables_renew_indices = list(zip_tablenames_set.intersection(tables_with_indices))

        if tables_renew_indices:
            renew_table_indices = {table_name: columns for table_name, columns in tables_with_indices.items() if table_name in tables_renew_indices}
            generate_tables_indices(self.database.engine, renew_table_indices)

    def run(self) -> None:
        """
        Runs the ETL process.
        """
        audits = self.retrieve_data()

        if audits:
            audit_metadata = create_audit_metadata(audits, self.download_folder)
            self.load_data(audit_metadata)
            self.create_indices(audit_metadata)
            self.insert_audits(audit_metadata)
            
            if self.delete_zips:
                remove_folder(self.download_folder)
        else: 
            logger.warn("No data to load!")
