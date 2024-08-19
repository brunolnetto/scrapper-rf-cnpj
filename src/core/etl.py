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
from core.utils.etl import (
    get_RF_data, 
    load_RF_data_on_database,
    generate_tables_indices,
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
        
    def scrap_data(self):
        """
        Scrapes the RF (Receita Federal) website to extract file information.

        Returns:
            list: A list of tuples containing the updated date and filename of the files found on the RF website.
        """
        raw_html = request.urlopen(self.data_url)
        raw_html = raw_html.read()

        # Formatar página e converter em string
        page_items = BeautifulSoup(raw_html, 'lxml')

        # Find all table rows
        table_rows = page_items.find_all('tr')
        
        # Extract data from each row
        files_info = []
        for row in table_rows:
            # Find cells containing filename (anchor tag) and date
            filename_cell = row.find('a')
            regex_pattern=r'\d{4}-\d{2}-\d{2}'
            collect_date=lambda text: text and re.search(regex_pattern, text)
            date_cell = row.find('td', text=collect_date)
            
            size_types=['K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
            or_map = lambda a, b: a or b
            is_size_type = lambda text: reduce(
                or_map, 
                [text.endswith(size_type) for size_type in size_types]
            )
            size_cell = row.find('td', text=lambda text: text and is_size_type(text))
            
            has_data = filename_cell and date_cell and size_cell
            if has_data:
                filename = filename_cell.text.strip()

                if filename.endswith('.zip'):
                    # Extract date and format it
                    date_text = date_cell.text.strip()

                    # Try converting date text to datetime object (adjust format if needed)
                    try:
                        updated_at = datetime.strptime(date_text, "%Y-%m-%d %H:%M")
                        sao_paulo_timezone = pytz.timezone("America/Sao_Paulo")
                        updated_at = sao_paulo_timezone.localize(updated_at)
                        
                        updated_at = updated_at.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    except ValueError:
                        # Handle cases where date format doesn't match
                        logger.error(f"Error parsing date for file: {filename}")

                    size_value_str = size_cell.text.strip()
                    
                    file_info = FileInfo(
                        filename=filename, 
                        updated_at=updated_at,
                        file_size=convert_to_bytes(size_value_str)
                    )
                    files_info.append(file_info)
        
        return files_info
    
    def get_data(self, audits: List[AuditDB]):
        """
        Downloads the files from the RF (Receita Federal) website.

        Returns:
            None
        """
        
        # Get data
        if audits:
            get_RF_data(
                self.data_url, self.layout_url, 
                audits, self.download_folder, self.extract_folder, 
                self.is_parallel
            )

    def audit_scrapped_files(self, files_info: List[FileInfo]):
        condition_map = lambda info: \
            info.filename.endswith('.zip') or \
            (
                info.filename.endswith('.pdf') and 
                re.match(r'layout', info.filename, re.IGNORECASE)
            )
        files_info=list(filter(condition_map, files_info))
    
        # Create file groups
        file_groups_info = create_file_groups(files_info)

        # Create audits
        audits = create_audits(self.database, file_groups_info)
        
        return audits

    def fetch_data(self):
        """
        Filters the data to be loaded into the database.

        Returns:
            None
        """
        # Scrap data
        files_info = self.scrap_data()

        # Audit scrapped files
        audits = self.audit_scrapped_files(files_info)
        
        return audits

    def retrieve_data(self):
        """
        Retrieves the data from the database.

        Returns:
            None
        """
        # Scrap data
        audits = self.fetch_data()
        
        # Test purpose only
        if getenv("ENVIRONMENT") == "development": 
            audits = list(
                filter(
                    lambda x: x.audi_file_size_bytes < 50000, 
                    sorted(audits, key=lambda x: x.audi_file_size_bytes)
                )
            )
        
        # Get data
        if audits:
            # Get data
            self.get_data(audits)
            return audits

        else:
            return []
            

    def load_data(self, audit_metadata: AuditMetadata):
        """
        Loads the data into the database.

        Returns:
            None
        """
        # Load database
        load_RF_data_on_database(self.database, self.extract_folder, audit_metadata)

    def insert_audits(self, audit_metadata: AuditMetadata):
        # Insert audit metadata
        for audit in audit_metadata.audit_list:
            try:
                insert_audit(self.database, audit)
            
            except Exception as e:
                logger.error(f"Error inserting audit {audit}: {e}")

    def load_without_download(self):
        """
        Uploads the data to the database without downloading it.

        Returns:
            None
        """
        audits = self.fetch_data()

        if audits:
            # Create audit metadata
            audit_metadata = create_audit_metadata(audits, self.download_folder)
            for audit in audit_metadata.audit_list:
                audit.audi_downloaded_at=datetime.now()
                audit.audi_processed_at=datetime.now()
            
            # Load data
            self.load_data(audit_metadata)

            # Create indices
            self.create_indices(self, audit_metadata)

            # Insert audit metadata
            self.insert_audits(audit_metadata)
        else: 
            logger.warn("No data to load!")
            
    def only_create_indices(self):
        audits = self.fetch_data()

        if audits:
            # Create audit metadata
            audit_metadata = create_audit_metadata(audits, self.download_folder)
            for audit in audit_metadata.audit_list:
                audit.audi_downloaded_at=datetime.now()
                audit.audi_processed_at=datetime.now()
                audit.audi_inserted_at=datetime.now()
            
            # Create indices
            self.create_indices(audit_metadata)
            
            # Insert audit metadata
            self.insert_audits(audit_metadata)
        else: 
            logger.warn("No data to load!")
        
            
    def create_indices(self, audit_metadata):
        table_to_filenames = audit_metadata.tablename_to_zipfile_to_files
        zip_tablenames_set = set(table_to_filenames.keys())
        
        table_to_filenames = audit_metadata.tablename_to_zipfile_to_files
        zip_tablenames_set = set(table_to_filenames.keys())

        # Generate tables indices
        tables_with_indices = {
            "estabelecimento": {
                "cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnae_principal", "cnae_secundaria", "cep", "uf"
            },
            "empresa": {
                "cnpj_basico"
            },
            "simples": {
                "cnpj_basico"
            }, 
            "socios": {"cnpj_basico"}
        }
        tables_renew_indices = list(zip_tablenames_set.intersection(tables_with_indices))

        has_new_tables = len(tables_renew_indices) != 0
        renew_table_indices = {
            table_name: columns
            for table_name, columns in tables_with_indices.items()
            if table_name in tables_renew_indices
        }
        
        if(has_new_tables):
            generate_tables_indices(self.database.engine, renew_table_indices)

    def run(self):
        """
        Runs the ETL process.

        Returns:
            None
        """
        audits = self.retrieve_data()

        if audits:
            # Create audit metadata
            audit_metadata = create_audit_metadata(audits, self.download_folder)

            # Load data
            self.load_data(audit_metadata)
            
            # Create indices
            self.create_indices(audit_metadata)

            # Insert audit metadata
            self.insert_audits(audit_metadata)
            
            # Delete download folder content
            if self.delete_zips:
                remove_folder(self.download_folder)
            
        else: 
            logger.warn("No data to load!")