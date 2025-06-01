import polars as pl
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getcwd, path

from core.constants import TABLES_INFO_DICT
from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL

def convert_table_csvs_to_parquet(table_name, csv_paths, output_dir):
    try:
        expected_columns = TABLES_INFO_DICT.get(table_name, {}).get("columns")
        if not expected_columns:
            return f"[WARN] No column mapping for '{table_name}'"

        schema = {col: pl.Utf8 for col in expected_columns}

        out_file = output_dir / f"{table_name}.parquet"
        if out_file.exists():
            out_file.unlink()

        dfs_lazy = [
            pl.scan_csv(str(filepath), schema=schema, encoding='utf8-lossy')
            for filepath in csv_paths
        ]
        if not dfs_lazy:
            return f"[WARN] No valid CSV files for '{table_name}'"

        df = pl.concat(dfs_lazy)
        df.sink_parquet(str(out_file))

        return f"[OK] Processed '{table_name}'"
    except Exception as e:
        return f"[ERROR] Failed '{table_name}': {e}"

def convert_csvs_to_parquet(audit_map: dict, unzip_dir: Path, output_dir: Path, max_workers: int = 4):
    output_dir.mkdir(exist_ok=True)

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task("[cyan]Processing tables", total=len(audit_map))

            for table_name, zip_map in audit_map.items():
                csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
                if not csv_paths:
                    continue
                tasks.append(executor.submit(
                    convert_table_csvs_to_parquet, 
                    table_name, csv_paths, output_dir
                ))

            for future in as_completed(tasks):
                result = future.result()
                print(result)
                progress.advance(task)

YEAR = 2025
MONTH = str(5).zfill(2)
FILES_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{YEAR}-{MONTH}"
LAYOUT_URL = "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"

download_folder = Path(path.join(getcwd(), "../data/DOWNLOAD_FILES"))
extract_folder = Path(path.join(getcwd(), "../data/EXTRACTED_FILES"))
parquet_folder = Path(path.join(getcwd(), "../data/PARQUET_FILES"))

database = init_database(f"dadosrfb_{YEAR}{MONTH}")

scraper = CNPJ_ETL(
    database, FILES_URL, LAYOUT_URL,
    download_folder, extract_folder,
    is_parallel=True, delete_zips=True
)
audits = scraper._prepare_audit_metadata(scraper.fetch_data()).tablename_to_zipfile_to_files

csvs_to_parquet(audits, extract_folder, parquet_folder, max_workers=6)
