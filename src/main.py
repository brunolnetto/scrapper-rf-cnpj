# Project: ETL - CNPJs da Receita Federal do Brasil
# Objective: Download, transform, and load Brazilian Federal Revenue CNPJ data

import time
from uuid import uuid4
from pathlib import Path
import logging
from itertools import islice
from datetime import datetime
from charset_normalizer import from_path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import duckdb
from duckdb import DuckDBPyConnection, ConversionException
import polars as pl
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL
from core.constants import TABLES_INFO_DICT 

# ─── Configuration ───
logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("cnpj_etl")
start_time = time.time()

def main():
    YEAR = 2025
    MONTH = str(5).zfill(2)
    DB_FILE = f"dadosrfb_{YEAR}{MONTH}.duckdb"
    PARQUET_DIR = Path("parquet_out")
    FILES_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{YEAR}-{MONTH}"
    LAYOUT_URL = "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"

    log.info(f"ETL start: {datetime.now():%Y-%m-%d %H:%M:%S}")

    # Phase 0: Download & extract
    download_folder, extract_folder = get_sink_folder()
    database = init_database(f"dadosrfb_{YEAR}{MONTH}")
    
    scraper = CNPJ_ETL(
        database, FILES_URL, LAYOUT_URL,
        download_folder, extract_folder,
        is_parallel=True, delete_zips=True
    )
    audits = scraper.run()

    elapsed = time.time() - start_time
    log.info(f"ETL complete in {elapsed:.2f}s — DB at {DB_FILE}")


if __name__ == "__main__":
    main()
