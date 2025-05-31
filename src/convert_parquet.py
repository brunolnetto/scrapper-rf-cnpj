import polars as pl
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

from pathlib import Path
from itertools import islice

from os import getcwd, path
from pathlib import Path

import polars as pl

from core.constants import TABLES_INFO_DICT
from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL

def csvs_to_parquet(audit_map: dict, unzip_dir: Path, output_dir: Path):
    """
    Convert CSVs under unzip_dir to one Parquet per key,
    selecting and ordering columns according to TABLES_INFO_DICT.
    """
    output_dir.mkdir(exist_ok=True)

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
    ) as prog:
        for table_name, zip_map in audit_map.items():
            csv_paths = [unzip_dir / fname for files in zip_map.values() for fname in files]
            if not csv_paths:
                print(f"No CSVs for '{table_name}', skipping.")
                continue

            expected_columns = TABLES_INFO_DICT.get(table_name, {}).get("columns")
            encoding = TABLES_INFO_DICT[table_name].get("encoding", "utf8-lossy")

            if not expected_columns:
                print(f"No column mapping found for '{table_name}', skipping.")
                continue

            task = prog.add_task(f"[green]{table_name}", total=1)  # total = 1 per table
            out_file = output_dir / f"{table_name}.parquet"
            if out_file.exists():
                out_file.unlink()

            try:
                schema = {col: pl.Utf8 for col in expected_columns}
                dfs_lazy = [
                    pl.scan_csv(str(filepath), schema=schema, encoding=encoding)
                    for filepath in csv_paths if filepath.exists()
                ]

                if not dfs_lazy:
                    print(f"No valid CSV files for '{table_name}', skipping.")
                    continue

                df = pl.concat([scan.collect() for scan in dfs_lazy])
                df.write_parquet(str(out_file))

                prog.update(task, advance=1)  # mark table as done

            except Exception as e:
                print(f"Error processing '{table_name}': {e}")

YEAR = 2025
MONTH = str(5).zfill(2)
FILES_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{YEAR}-{MONTH}"
LAYOUT_URL = "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"

download_folder=Path(path.join(getcwd(), "../data/DOWNLOAD_FILES"))
extract_folder=Path(path.join(getcwd(), "../data/EXTRACTED_FILES"))
parquet_folder=Path(path.join(getcwd(), "../data/PARQUET_FILES"))

database = init_database(f"dadosrfb_{YEAR}{MONTH}")

scraper = CNPJ_ETL(
    database, FILES_URL, LAYOUT_URL,
    download_folder, extract_folder,
    is_parallel=True, delete_zips=True
)
audits = scraper._prepare_audit_metadata(scraper.fetch_data()).tablename_to_zipfile_to_files

csvs_to_parquet(audits, download_folder, parquet_folder)