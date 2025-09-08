import polars as pl
from sqlalchemy import create_engine, text
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging
import random
import string
from datetime import datetime, timedelta
import subprocess
import tempfile
import os
import time
import psutil
import json
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Data class to store performance metrics"""
    method: str
    file_path: Path
    table_name: str
    rows_loaded: int
    columns: int
    load_time_seconds: float
    memory_peak_mb: float
    throughput_mb_per_second: float
    throughput_rows_per_second: float
    success: bool
    error_message: Optional[str] = None

class PerformanceTracker:
    """Track memory usage and timing during operations"""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = None
        self.start_memory = None
        self.peak_memory = 0
    
    def start(self):
        """Start tracking"""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
        self.peak_memory = self.start_memory
    
    def update_peak_memory(self):
        """Update peak memory usage"""
        current_memory = self.process.memory_info().rss / (1024 * 1024)  # MB
        self.peak_memory = max(self.peak_memory, current_memory)
    
    def stop(self) -> tuple[float, float]:
        """Stop tracking and return elapsed time and peak memory"""
        elapsed_time = time.time() - self.start_time
        self.update_peak_memory()
        return elapsed_time, self.peak_memory - self.start_memory

class PerformanceTester:
    """Main class for performance testing CSV vs Parquet loading methods"""
    
    def __init__(self, connection_string: str, test_dir: Path = Path("performance_test_data")):
        self.connection_string = connection_string
        self.test_dir = test_dir
        self.duckdb_available = False
        self._parse_connection_string()
        self._check_dependencies()
    
    def _parse_connection_string(self):
        """Parse PostgreSQL connection string into components"""
        if self.connection_string.startswith('postgresql://'):
            parts = self.connection_string.replace('postgresql://', '').split('@')
            if len(parts) != 2:
                raise ValueError("Invalid PostgreSQL connection string format")
            
            user_pass = parts[0].split(':')
            host_port_db = parts[1].split('/')
            
            if len(user_pass) != 2 or len(host_port_db) != 2:
                raise ValueError("Invalid PostgreSQL connection string format")
            
            self.user, self.password = user_pass
            host_port, self.database = host_port_db
            
            if ':' in host_port:
                self.host, self.port = host_port.split(':')
            else:
                self.host, self.port = host_port, '5432'
        else:
            raise ValueError("Only SQLAlchemy connection string format supported")
    
    def _check_dependencies(self):
        """Check and install required dependencies"""
        print("\n🔧 Checking dependencies...")
        
        # Check pyarrow
        try:
            import pyarrow
            print("✅ pyarrow is available for Parquet support")
        except ImportError:
            print("❌ pyarrow not found. Installing...")
            self._install_package("pyarrow")
            print("✅ pyarrow installed")
        
        # Check duckdb
        try:
            import duckdb
            print("✅ duckdb is available for fast data processing")
            self.duckdb_available = True
        except ImportError:
            print("❌ duckdb not found. Attempting to install...")
            if self._install_package("duckdb"):
                self.duckdb_available = True
            else:
                print("⚠️  Failed to install duckdb automatically")
                print("DuckDB tests will be skipped. You can still run SQLAlchemy tests.")
                print("To install manually:")
                print("  uv pip install duckdb")
                print("  or")
                print("  pip install duckdb --break-system-packages")
                self.duckdb_available = False
    
    def _install_package(self, package_name: str) -> bool:
        """Install a Python package using uv or pip"""
        import subprocess
        try:
            subprocess.run(["uv", "pip", "install", package_name], check=True)
            print(f"✅ {package_name} installed via uv")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            try:
                subprocess.run(["pip", "install", package_name, "--break-system-packages"], check=True)
                print(f"✅ {package_name} installed via pip")
                return True
            except subprocess.CalledProcessError:
                print(f"❌ Failed to install {package_name}")
                return False
            
    def test_database_connection(self) -> bool:
        """Test if the database connection is working"""
        try:
            test_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', 'SELECT 1;']
            result = subprocess.run(test_cmd, check=True, env={'PGPASSWORD': self.password}, capture_output=True, text=True)
            return True
    except Exception as e:
            logger.error(f"Database connection test failed: {e}")
        return False

    def create_empresa_sample_data(self, num_rows: int = 100000, file_size_mb: Optional[int] = None) -> pl.DataFrame:
        """Create realistic empresa (company) sample data"""
        logger.info(f"Creating empresa sample data with {num_rows:,} rows")
        
        # Realistic CNPJ data structure for empresa table
        data = {
            'cnpj_basico': [f"{random.randint(10000000, 99999999):08d}" for _ in range(num_rows)],
            'razao_social': [f"EMPRESA {random.randint(1, 9999):04d} LTDA" for _ in range(num_rows)],
            'natureza_juridica': [f"{random.randint(1000, 9999):04d}" for _ in range(num_rows)],
            'qualificacao_responsavel': [f"{random.randint(10, 99):02d}" for _ in range(num_rows)],
            'capital_social': [round(random.uniform(1000, 10000000), 2) for _ in range(num_rows)],
            'porte_empresa': [random.choice(['01', '02', '03', '04', '05']) for _ in range(num_rows)],
            'ente_federativo_responsavel': [f"{random.randint(1000, 9999):04d}" for _ in range(num_rows)]
        }
        
        df = pl.DataFrame(data)
        
        # Adjust size if needed
        if file_size_mb:
            df = self._adjust_dataframe_size(df, file_size_mb, "empresa")
        
        return df
    
    def create_estabelecimento_sample_data(self, num_rows: int = 100000, file_size_mb: Optional[int] = None) -> pl.DataFrame:
        """Create realistic estabelecimento (establishment) sample data"""
        logger.info(f"Creating estabelecimento sample data with {num_rows:,} rows")
        
        # Realistic CNPJ data structure for estabelecimento table
        data = {
            'cnpj_basico': [f"{random.randint(10000000, 99999999):08d}" for _ in range(num_rows)],
            'cnpj_ordem': [f"{random.randint(1, 999):03d}" for _ in range(num_rows)],
            'cnpj_dv': [f"{random.randint(0, 9):01d}" for _ in range(num_rows)],
            'identificador_matriz_filial': [random.choice(['1', '2']) for _ in range(num_rows)],
            'nome_fantasia': [f"FANTASIA {random.randint(1, 9999):04d}" for _ in range(num_rows)],
            'situacao_cadastral': [random.choice(['01', '02', '03', '04', '05', '06', '07', '08']) for _ in range(num_rows)],
            'data_situacao_cadastral': [f"{random.randint(2010, 2024):04d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}" for _ in range(num_rows)],
            'motivo_situacao_cadastral': [f"{random.randint(1, 99):02d}" for _ in range(num_rows)],
            'nome_cidade_exterior': [f"CIDADE {random.randint(1, 999):03d}" for _ in range(num_rows)],
            'pais': [f"{random.randint(1, 999):03d}" for _ in range(num_rows)],
            'data_inicio_atividade': [f"{random.randint(2010, 2024):04d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}" for _ in range(num_rows)],
            'cnae_fiscal_principal': [f"{random.randint(1000000, 9999999):07d}" for _ in range(num_rows)],
            'cnae_fiscal_secundaria': [f"{random.randint(1000000, 9999999):07d}" for _ in range(num_rows)],
            'tipo_logradouro': [random.choice(['RUA', 'AVENIDA', 'TRAVESSA', 'PRACA']) for _ in range(num_rows)],
            'logradouro': [f"LOGRADOURO {random.randint(1, 9999):04d}" for _ in range(num_rows)],
            'numero': [f"{random.randint(1, 9999):04d}" for _ in range(num_rows)],
            'complemento': [f"COMPL {random.randint(1, 999):03d}" for _ in range(num_rows)],
            'bairro': [f"BAIRRO {random.randint(1, 999):03d}" for _ in range(num_rows)],
            'cep': [f"{random.randint(10000000, 99999999):08d}" for _ in range(num_rows)],
            'uf': [random.choice(['SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'PE', 'CE', 'GO']) for _ in range(num_rows)],
            'municipio': [f"{random.randint(1000, 9999):04d}" for _ in range(num_rows)],
            'ddd_1': [f"{random.randint(10, 99):02d}" for _ in range(num_rows)],
            'telefone_1': [f"{random.randint(10000000, 99999999):08d}" for _ in range(num_rows)],
            'ddd_2': [f"{random.randint(10, 99):02d}" for _ in range(num_rows)],
            'telefone_2': [f"{random.randint(10000000, 99999999):08d}" for _ in range(num_rows)],
            'ddd_fax': [f"{random.randint(10, 99):02d}" for _ in range(num_rows)],
            'fax': [f"{random.randint(10000000, 99999999):08d}" for _ in range(num_rows)],
            'correio_eletronico': [f"email{random.randint(1, 9999):04d}@empresa.com.br" for _ in range(num_rows)],
            'situacao_especial': [f"{random.randint(1, 99):02d}" for _ in range(num_rows)],
            'data_situacao_especial': [f"{random.randint(2010, 2024):04d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}" for _ in range(num_rows)]
        }
        
        df = pl.DataFrame(data)
        
        # Adjust size if needed
        if file_size_mb:
            df = self._adjust_dataframe_size(df, file_size_mb, "estabelecimento")
        
        return df
    
    def create_socios_sample_data(self, num_rows: int = 100000, file_size_mb: Optional[int] = None) -> pl.DataFrame:
        """Create realistic socios (partners) sample data"""
        logger.info(f"Creating socios sample data with {num_rows:,} rows")
        
        # Realistic CNPJ data structure for socios table
        data = {
            'cnpj_basico': [f"{random.randint(10000000, 99999999):08d}" for _ in range(num_rows)],
            'identificador_socio': [random.choice(['1', '2']) for _ in range(num_rows)],
            'nome_socio_razao_social': [f"SOCIO {random.randint(1, 9999):04d}" for _ in range(num_rows)],
            'cpf_cnpj_socio': [f"{random.randint(10000000000, 99999999999):011d}" for _ in range(num_rows)],
            'qualificacao_socio': [f"{random.randint(10, 99):02d}" for _ in range(num_rows)],
            'data_entrada_sociedade': [f"{random.randint(2010, 2024):04d}{random.randint(1, 12):02d}{random.randint(1, 28):02d}" for _ in range(num_rows)],
            'pais': [f"{random.randint(1, 999):03d}" for _ in range(num_rows)],
            'representante_legal': [random.choice(['S', 'N']) for _ in range(num_rows)],
            'nome_do_representante': [f"REPRESENTANTE {random.randint(1, 9999):04d}" for _ in range(num_rows)],
            'qualificacao_representante_legal': [f"{random.randint(10, 99):02d}" for _ in range(num_rows)],
            'faixa_etaria': [random.choice(['1', '2', '3', '4', '5', '6']) for _ in range(num_rows)]
        }
        
        df = pl.DataFrame(data)
        
        # Adjust size if needed
        if file_size_mb:
            df = self._adjust_dataframe_size(df, file_size_mb, "socios")
        
        return df
    
    def create_cnae_sample_data(self, num_rows: int = 100000, file_size_mb: Optional[int] = None) -> pl.DataFrame:
        """Create realistic CNAE sample data"""
        logger.info(f"Creating CNAE sample data with {num_rows:,} rows")
        
        # Realistic CNAE data structure
        cnae_codes = [
            "4751201", "4751202", "4752100", "4753001", "4753002", "4753003", "4753004", "4753005",
            "4753006", "4753007", "4753008", "4753009", "4754000", "4755007", "4755008", "4755009",
            "4756006", "4756007", "4756008", "4756009", "4757005", "4757006", "4757007", "4757008",
            "4757009", "4758004", "4758005", "4758006", "4758007", "4758008", "4758009", "4759003"
        ]
        
        cnae_descriptions = [
            "Comércio varejista de informática", "Assistência técnica de informática",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de livros",
            "Comércio varejista de jornais e revistas", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria",
            "Comércio varejista de artigos de papelaria", "Comércio varejista de artigos de papelaria"
        ]
        
        data = {
            'codigo': [random.choice(cnae_codes) for _ in range(num_rows)],
            'descricao': [random.choice(cnae_descriptions) for _ in range(num_rows)]
        }
        
    df = pl.DataFrame(data)
    
        # Adjust size if needed
    if file_size_mb:
            df = self._adjust_dataframe_size(df, file_size_mb, "cnae")
        
        return df
    
    def _adjust_dataframe_size(self, df: pl.DataFrame, target_size_mb: int, table_type: str) -> pl.DataFrame:
        """Adjust dataframe size to achieve target file size"""
        temp_path = Path(f"temp_size_check_{table_type}.parquet")
        df.write_parquet(temp_path)
        current_size_mb = temp_path.stat().st_size / (1024 * 1024)
        temp_path.unlink()
        
        if current_size_mb > 0:
            target_rows = int(len(df) * (target_size_mb / current_size_mb))
            logger.info(f"Adjusting {table_type} to {target_rows:,} rows to achieve ~{target_size_mb}MB file size")
            
            # Duplicate data to reach target size
            if target_rows > len(df):
                multiplier = target_rows // len(df) + 1
                df = pl.concat([df] * multiplier)
                df = df.head(target_rows)
            else:
                df = df.head(target_rows)
        
        return df
    
    def create_test_files(self, file_sizes_mb: List[int] = [10, 50, 100], table_types: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Create realistic test files in both CSV and Parquet formats"""
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
        if table_types is None:
            table_types = ['empresa', 'estabelecimento', 'socios', 'cnae']
        
        test_files = {}
        
        for table_type in table_types:
            for size_mb in file_sizes_mb:
                parquet_path = self.test_dir / f"{table_type}_{size_mb}mb.parquet"
                csv_path = self.test_dir / f"{table_type}_{size_mb}mb.csv"
                
                # Check if both files already exist
                if parquet_path.exists() and csv_path.exists():
                    logger.info(f"Test files for {table_type} {size_mb}MB already exist, skipping creation...")
                    
                    # Get file sizes and basic info
                    parquet_size = parquet_path.stat().st_size / (1024 * 1024)
                    csv_size = csv_path.stat().st_size / (1024 * 1024)
                    
                    # Read a small sample to get row count
                    try:
                        df_sample = pl.read_parquet(parquet_path, n_rows=1)
                        # Estimate rows based on file size ratio
                        estimated_rows = int((parquet_size / size_mb) * 100000)  # Rough estimate
                    except Exception:
                        estimated_rows = 0
                    
                    test_files[f"{table_type}_{size_mb}mb"] = {
                        'table_type': table_type,
                        'parquet': parquet_path,
                        'csv': csv_path,
                        'parquet_size_mb': parquet_size,
                        'csv_size_mb': csv_size,
                        'rows': estimated_rows,
                        'columns': len(df_sample.columns) if 'df_sample' in locals() else 0
                    }
                    
                    logger.info(f"Using existing {table_type} {size_mb}MB test files:")
                    logger.info(f"  Parquet: {parquet_size:.2f}MB (~{estimated_rows:,} rows)")
                    logger.info(f"  CSV: {csv_size:.2f}MB (~{estimated_rows:,} rows)")
                    
                else:
                    logger.info(f"Creating {table_type} test files for {size_mb}MB target size...")
                    
                    # Create sample data based on table type
                    if table_type == 'empresa':
                        df = self.create_empresa_sample_data(file_size_mb=size_mb)
                    elif table_type == 'estabelecimento':
                        df = self.create_estabelecimento_sample_data(file_size_mb=size_mb)
                    elif table_type == 'socios':
                        df = self.create_socios_sample_data(file_size_mb=size_mb)
                    elif table_type == 'cnae':
                        df = self.create_cnae_sample_data(file_size_mb=size_mb)
                    else:
                        logger.warning(f"Unknown table type: {table_type}, skipping...")
                        continue
                    
                    # Create Parquet file
                    df.write_parquet(parquet_path)
                    parquet_size = parquet_path.stat().st_size / (1024 * 1024)
                    
                    # Create CSV file
                    df.write_csv(csv_path)
                    csv_size = csv_path.stat().st_size / (1024 * 1024)
                    
                    test_files[f"{table_type}_{size_mb}mb"] = {
                        'table_type': table_type,
                        'parquet': parquet_path,
                        'csv': csv_path,
                        'parquet_size_mb': parquet_size,
                        'csv_size_mb': csv_size,
                        'rows': len(df),
                        'columns': len(df.columns)
                    }
                    
                    logger.info(f"Created {table_type} {size_mb}MB test files:")
                    logger.info(f"  Parquet: {parquet_size:.2f}MB ({len(df):,} rows)")
                    logger.info(f"  CSV: {csv_size:.2f}MB ({len(df):,} rows)")
        
        return test_files
    
    def load_csv_to_postgres_sqlalchemy(self, csv_file: Path, table_name: str, if_exists: str = 'replace') -> tuple[bool, Optional[str]]:
        """Load CSV to PostgreSQL using SQLAlchemy + polars"""
        tracker = PerformanceTracker()
        tracker.start()
        
        try:
            # Create SQLAlchemy engine
            engine = create_engine(self.connection_string)
            
            # Read CSV with polars
            logger.info(f"Reading CSV file: {csv_file}")
            df = pl.read_csv(csv_file)
            
            tracker.update_peak_memory()
            
            logger.info(f"Loading {len(df)} rows into table '{table_name}'")
            
            # Convert to pandas for SQLAlchemy compatibility (temporary solution)
            # TODO: Replace with direct polars-to-postgres when connectorx supports it
            pandas_df = df.to_pandas()
            
            # Load to PostgreSQL using pandas to_sql
            pandas_df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method='multi',  # Use PostgreSQL COPY
                chunksize=10000
            )
            
            elapsed_time, memory_used = tracker.stop()
            logger.info(f"CSV SQLAlchemy load completed in {elapsed_time:.2f}s")
            return True, None
            
        except Exception as e:
            elapsed_time, memory_used = tracker.stop()
            error_msg = f"Error loading CSV via SQLAlchemy: {e}"
            logger.error(error_msg)
            return False, error_msg
        finally:
            if 'engine' in locals():
                engine.dispose()
    
    def load_csv_to_postgres_duckdb(self, csv_file: Path, table_name: str, if_exists: str = 'replace') -> tuple[bool, Optional[str]]:
        """Load CSV to PostgreSQL using DuckDB + COPY"""
        tracker = PerformanceTracker()
        tracker.start()
        
        try:
            import duckdb
            
            # Handle table existence
            if if_exists == 'replace':
                drop_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', f'DROP TABLE IF EXISTS {table_name};']
                subprocess.run(drop_cmd, check=True, env={'PGPASSWORD': self.password})
                logger.info(f"Dropped existing table '{table_name}'")
            
            # Create a simple table structure
            # First, get a sample of the CSV to determine column count using DuckDB Python API
            con = duckdb.connect()
            sample_df = con.execute(f"SELECT * FROM read_csv_auto('{csv_file.absolute()}') LIMIT 1").df()
            con.close()
            
            # Create table with appropriate number of columns
            column_count = len(sample_df.columns)
            columns = [f"col_{i} TEXT" for i in range(column_count)]
            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
            
            create_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', create_table_sql]
            subprocess.run(create_cmd, check=True, env={'PGPASSWORD': self.password})
            
            # Copy data using DuckDB Python API -> CSV -> PostgreSQL COPY
            logger.info(f"Starting DuckDB -> PostgreSQL COPY for CSV table '{table_name}'")
            
            # Use DuckDB Python API to read CSV and output to CSV string
            con = duckdb.connect()
            csv_data = con.execute(f"SELECT * FROM read_csv_auto('{csv_file.absolute()}')").df().to_csv(index=False, header=False)
            con.close()
            
            # Write CSV data to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
                temp_csv.write(csv_data)
                temp_csv_path = temp_csv.name
            
            try:
                # Use psql \copy (client-side) instead of COPY (server-side)
                copy_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', f'\\copy {table_name} FROM \'{temp_csv_path}\' WITH (FORMAT CSV);']
                subprocess.run(copy_cmd, check=True, env={'PGPASSWORD': self.password})
                
                elapsed_time, _ = tracker.stop()
                logger.info(f"CSV DuckDB load completed in {elapsed_time:.2f}s")
                return True, None
                
            finally:
                # Clean up temporary file
                os.unlink(temp_csv_path)
            
        except Exception as e:
            elapsed_time, _ = tracker.stop()
            error_msg = f"Error loading CSV via DuckDB: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def load_parquet_to_postgres_sqlalchemy(self, parquet_file: Path, table_name: str, if_exists: str = 'replace') -> tuple[bool, Optional[str]]:
        """Load Parquet to PostgreSQL using SQLAlchemy + polars"""
        tracker = PerformanceTracker()
        tracker.start()
        
        try:
            # Create SQLAlchemy engine
            engine = create_engine(self.connection_string)
            
            # Read Parquet with polars
            logger.info(f"Reading Parquet file: {parquet_file}")
            df = pl.read_parquet(parquet_file)
            
            tracker.update_peak_memory()
            
            logger.info(f"Loading {len(df)} rows into table '{table_name}'")
            
            # Convert to pandas for SQLAlchemy compatibility (temporary solution)
            # TODO: Replace with direct polars-to-postgres when connectorx supports it
            pandas_df = df.to_pandas()
            
            # Load to PostgreSQL using pandas to_sql
            pandas_df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method='multi',  # Use PostgreSQL COPY
                chunksize=10000
            )
            
            elapsed_time, memory_used = tracker.stop()
            logger.info(f"Parquet SQLAlchemy load completed in {elapsed_time:.2f}s")
            return True, None
            
        except Exception as e:
            elapsed_time, memory_used = tracker.stop()
            error_msg = f"Error loading Parquet via SQLAlchemy: {e}"
            logger.error(error_msg)
            return False, error_msg
        finally:
            if 'engine' in locals():
                engine.dispose()

    def load_parquet_to_postgres_duckdb(self, parquet_file: Path, table_name: str, if_exists: str = 'replace') -> tuple[bool, Optional[str]]:
        """Load Parquet to PostgreSQL using DuckDB + COPY"""
        tracker = PerformanceTracker()
        tracker.start()
        
        try:
            import duckdb
            
            # Handle table existence
            if if_exists == 'replace':
                drop_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', f'DROP TABLE IF EXISTS {table_name};']
                subprocess.run(drop_cmd, check=True, env={'PGPASSWORD': self.password})
                logger.info(f"Dropped existing table '{table_name}'")
            
            # Create a simple table structure
            # First, get a sample of the Parquet to determine column count using DuckDB Python API
            con = duckdb.connect()
            sample_df = con.execute(f"SELECT * FROM read_parquet('{parquet_file.absolute()}') LIMIT 1").df()
            con.close()
            
            # Create table with appropriate number of columns
            column_count = len(sample_df.columns)
            columns = [f"col_{i} TEXT" for i in range(column_count)]
            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
            
            create_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', create_table_sql]
            subprocess.run(create_cmd, check=True, env={'PGPASSWORD': self.password})
            
            # Copy data using DuckDB Python API -> CSV -> PostgreSQL COPY
            logger.info(f"Starting DuckDB -> PostgreSQL COPY for Parquet table '{table_name}'")
            
            # Use DuckDB Python API to read Parquet and output to CSV string
            con = duckdb.connect()
            csv_data = con.execute(f"SELECT * FROM read_parquet('{parquet_file.absolute()}')").df().to_csv(index=False, header=False)
            con.close()
            
            # Write CSV data to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as temp_csv:
                temp_csv.write(csv_data)
                temp_csv_path = temp_csv.name
            
            try:
                # Use psql \copy (client-side) instead of COPY (server-side)
                copy_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', f'\\copy {table_name} FROM \'{temp_csv_path}\' WITH (FORMAT CSV);']
                subprocess.run(copy_cmd, check=True, env={'PGPASSWORD': self.password})
                
                elapsed_time, _ = tracker.stop()
                logger.info(f"Parquet DuckDB load completed in {elapsed_time:.2f}s")
                return True, None
                
            finally:
                # Clean up temporary file
                os.unlink(temp_csv_path)
            
        except Exception as e:
            elapsed_time, _ = tracker.stop()
            error_msg = f"Error loading Parquet via DuckDB: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    def check_existing_tables(self, table_prefix: str) -> List[str]:
        """Check for existing test tables with the given prefix"""
        try:
            # Query for existing tables
            query_cmd = [
                'psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, 
                '-c', f"SELECT tablename FROM pg_tables WHERE tablename LIKE '{table_prefix}%';"
            ]
            
            result = subprocess.run(query_cmd, check=True, env={'PGPASSWORD': self.password}, capture_output=True, text=True)
            
            # Parse output to get table names
            lines = result.stdout.strip().split('\n')
            table_names = []
            for line in lines:
                line = line.strip()
                # Skip header, separator lines, and empty lines
                if (line and 
                    not line.startswith('tablename') and 
                    not line.startswith('---') and 
                    not line.startswith('(') and  # Skip row count lines like "(0 rows)"
                    not line.startswith('row')):
                    table_names.append(line)
            
            return table_names
            
        except Exception as e:
            logger.error(f"Error checking existing tables: {e}")
            return []
    
    def cleanup_test_tables(self, table_names: List[str]):
        """Clean up test tables from the database"""
        try:
            # Drop each table
            for table_name in table_names:
                try:
                    drop_cmd = ['psql', '-h', self.host, '-p', self.port, '-U', self.user, '-d', self.database, '-c', f'DROP TABLE IF EXISTS {table_name};']
                    result = subprocess.run(drop_cmd, check=True, env={'PGPASSWORD': self.password}, capture_output=True, text=True)
                except subprocess.CalledProcessError as e:
                    logger.warning(f"Failed to drop table {table_name}: {e}")
            
            logger.info(f"Successfully cleaned up {len(table_names)} test tables")
            
        except Exception as e:
            logger.error(f"Error during table cleanup: {e}")
    
    def run_performance_test(self, test_files: Dict[str, Dict[str, Any]], table_prefix: str = "perf_test", cleanup_tables: bool = True) -> List[PerformanceMetrics]:
        """Run comprehensive performance tests"""
        metrics = []
        
        # Test methods
        methods = [
            ('csv', 'sqlalchemy', self.load_csv_to_postgres_sqlalchemy),
            ('parquet', 'sqlalchemy', self.load_parquet_to_postgres_sqlalchemy),
        ]
        
        # Add DuckDB methods only if available
        if self.duckdb_available:
            methods.extend([
                ('csv', 'duckdb', self.load_csv_to_postgres_duckdb),
                ('parquet', 'duckdb', self.load_parquet_to_postgres_duckdb),
            ])
        
        # Track all created tables for cleanup
        created_tables = []
        
        try:
            for file_key, file_info in test_files.items():
                table_type = file_info['table_type']
                logger.info(f"\n{'='*60}")
                logger.info(f"Testing {table_type} files")
                logger.info(f"{'='*60}")
                
                for file_type, method_name, load_function in methods:
                    logger.info(f"\n--- Testing {file_type.upper()} with {method_name.upper()} ---")
                    
                    file_path = file_info[file_type]
                    file_size = file_info[f'{file_type}_size_mb']
                    rows = file_info['rows']
                    columns = file_info['columns']
                    
                    # Create table name with table type included
                    table_name = f"{table_prefix}_{table_type}_{file_type}_{method_name}"
                    created_tables.append(table_name)
                    
                    # Run the test
                    tracker = PerformanceTracker()
                    tracker.start()
                    
                    success, error = load_function(file_path, table_name)
                    
                    elapsed_time, memory_used = tracker.stop()
                    
                    # Calculate throughput
                    throughput_mb_per_second = file_size / elapsed_time if elapsed_time > 0 else 0
                    throughput_rows_per_second = rows / elapsed_time if elapsed_time > 0 else 0
                    
                    # Create metrics
                    metric = PerformanceMetrics(
                        method=method_name,
                        file_path=file_path,
            table_name=table_name,
                        rows_loaded=rows,
                        columns=columns,
                        load_time_seconds=elapsed_time,
                        memory_peak_mb=memory_used,
                        throughput_mb_per_second=throughput_mb_per_second,
                        throughput_rows_per_second=throughput_rows_per_second,
                        success=success,
                        error_message=error,
                    )
                    
                    metrics.append(metric)
                    
                    # Log results
                    if success:
                        logger.info(f"✅ SUCCESS: {elapsed_time:.2f}s, {memory_used:.1f}MB peak, {throughput_mb_per_second:.1f}MB/s")
                    else:
                        logger.error(f"❌ FAILED: {error}")
        
        finally:
            # Cleanup test tables if requested
            if cleanup_tables and created_tables:
                logger.info(f"\n🧹 Cleaning up {len(created_tables)} test tables...")
                self.cleanup_test_tables(created_tables)
        
        return metrics
    
    def print_performance_report(self, metrics: List[PerformanceMetrics]):
        """Print a comprehensive performance report"""
        print("\n" + "="*80)
        print("CNPJ PERFORMANCE TEST RESULTS")
        print("="*80)
        
        # Group by table type first, then by file size
        by_table_type = {}
        for metric in metrics:
            # Extract table type from table name (e.g., "perf_test_empresa_10mb" -> "empresa")
            table_name = metric.table_name
            if '_' in table_name:
                parts = table_name.split('_')
                if len(parts) >= 3:
                    table_type = parts[2]  # Get table type from name
                else:
                    table_type = "unknown"
            else:
                table_type = "unknown"
            
            if table_type not in by_table_type:
                by_table_type[table_type] = {}
            
            # Extract file size from file path
            file_name = metric.file_path.name
            if '_' in file_name:
                size_part = file_name.split('_')[-1].replace('.parquet', '').replace('.csv', '')
                size_key = size_part.upper()
            else:
                size_key = "unknown"
            
            if size_key not in by_table_type[table_type]:
                by_table_type[table_type][size_key] = []
            by_table_type[table_type][size_key].append(metric)
        
        # Print results grouped by table type
        for table_type, size_groups in by_table_type.items():
            print(f"\n📊 RESULTS FOR {table_type.upper()} TABLE:")
            print("=" * 60)
            
            for size_key, size_metrics in size_groups.items():
                print(f"\n  📁 {size_key} files:")
                print("  " + "-" * 50)
                
                # Sort by load time (fastest first)
                sorted_metrics = sorted(size_metrics, key=lambda x: x.load_time_seconds)
                
                for i, metric in enumerate(sorted_metrics):
                    status = "✅" if metric.success else "❌"
                    # Extract file type from file path
                    file_type = metric.file_path.suffix.replace('.', '').upper()
                    method_display = f"{file_type}-{metric.method.upper()}"
                    
                    print(f"  {i+1:2d}. {status} {method_display:20s} | "
                          f"Time: {metric.load_time_seconds:6.2f}s | "
                          f"Speed: {metric.throughput_mb_per_second:6.1f}MB/s | "
                          f"Memory: {metric.memory_peak_mb:6.1f}MB")
                    
                    if not metric.success:
                        print(f"      Error: {metric.error_message}")
        
        # Overall summary statistics
        print(f"\n📈 OVERALL SUMMARY:")
        print("=" * 60)
        
        successful_metrics = [m for m in metrics if m.success]
        if successful_metrics:
            fastest = min(successful_metrics, key=lambda x: x.load_time_seconds)
            slowest = max(successful_metrics, key=lambda x: x.load_time_seconds)
            
            fastest_file_type = fastest.file_path.suffix.replace('.', '').upper()
            slowest_file_type = slowest.file_path.suffix.replace('.', '').upper()
            
            print(f"🏆 Fastest overall: {fastest_file_type}-{fastest.method.upper()} "
                  f"({fastest.load_time_seconds:.2f}s, {fastest.throughput_mb_per_second:.1f}MB/s)")
            print(f"🐌 Slowest overall: {slowest_file_type}-{slowest.method.upper()} "
                  f"({slowest.load_time_seconds:.2f}s, {slowest.throughput_mb_per_second:.1f}MB/s)")
            
            # Average performance by method
            method_stats = {}
            for metric in successful_metrics:
                file_type = metric.file_path.suffix.replace('.', '').upper()
                key = f"{file_type}-{metric.method}"
                if key not in method_stats:
                    method_stats[key] = []
                method_stats[key].append(metric)
            
            print(f"\n📊 AVERAGE PERFORMANCE BY METHOD:")
            print("-" * 60)
            for method, method_metrics in method_stats.items():
                avg_time = sum(m.load_time_seconds for m in method_metrics) / len(method_metrics)
                avg_throughput = sum(m.throughput_mb_per_second for m in method_metrics) / len(method_metrics)
                avg_memory = sum(m.memory_peak_mb for m in method_metrics) / len(method_metrics)
                
                print(f"{method.upper():20s} | "
                      f"Avg Time: {avg_time:6.2f}s | "
                      f"Avg Speed: {avg_throughput:6.1f}MB/s | "
                      f"Avg Memory: {avg_memory:6.1f}MB")
            
            # Performance by file type
            print(f"\n📊 PERFORMANCE BY FILE TYPE:")
            print("-" * 60)
            file_type_stats = {}
            for metric in successful_metrics:
                file_type = metric.file_path.suffix.replace('.', '').upper()
                if file_type not in file_type_stats:
                    file_type_stats[file_type] = []
                file_type_stats[file_type].append(metric)
            
            for file_type, file_metrics in file_type_stats.items():
                avg_time = sum(m.load_time_seconds for m in file_metrics) / len(file_metrics)
                avg_throughput = sum(m.throughput_mb_per_second for m in file_metrics) / len(file_metrics)
                avg_memory = sum(m.memory_peak_mb for m in file_metrics) / len(file_metrics)
                
                print(f"{file_type:12s} | "
                      f"Avg Time: {avg_time:6.2f}s | "
                      f"Avg Speed: {avg_throughput:6.1f}MB/s | "
                      f"Avg Memory: {avg_memory:6.1f}MB")
        
        # Test summary
        print(f"\n📋 TEST SUMMARY:")
        print("-" * 60)
        print(f"Total tests: {len(metrics)}")
        print(f"Successful: {len(successful_metrics)}")
        print(f"Failed: {len(metrics) - len(successful_metrics)}")
        
        if successful_metrics:
            success_rate = (len(successful_metrics) / len(metrics)) * 100
            print(f"Success rate: {success_rate:.1f}%")
    
    def save_performance_report(self, metrics: List[PerformanceMetrics], output_file: Path):
        """Save performance metrics to JSON file"""
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'metrics': [
                {
                    'method': m.method,
                    'file_path': m.file_path.name, # Save file path name
                    'table_name': m.table_name,
                    'rows_loaded': m.rows_loaded,
                    'columns': m.columns,
                    'load_time_seconds': m.load_time_seconds,
                    'memory_peak_mb': m.memory_peak_mb,
                    'throughput_mb_per_second': m.throughput_mb_per_second,
                    'throughput_rows_per_second': m.throughput_rows_per_second,
                    'success': m.success,
                    'error_message': m.error_message,
                }
                for m in metrics
            ]
        }
        
        with open(output_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logger.info(f"Performance report saved to: {output_file}")

# Main execution
if __name__ == "__main__":
    # Configuration
    connection_string = "postgresql://postgres:postgres@localhost:5432/postgres"
    test_dir = Path("performance_test_data")
    report_file = Path("performance_report.json")
    table_prefix = "perf_test"
    
    # File sizes to test (in MB) - Realistic sizes for CNPJ data
    file_sizes = [10, 25, 50]  # Realistic sizes for CNPJ tables
    
    # Table types to test (realistic CNPJ table types)
    table_types = ['empresa', 'estabelecimento', 'socios', 'cnae']
    
    print("🚀 CNPJ PARQUET vs CSV PERFORMANCE TESTING SUITE")
    print("=" * 70)
    print(f"Testing realistic CNPJ data structures:")
    for table_type in table_types:
        print(f"  📊 {table_type}")
    print(f"File sizes: {file_sizes}MB")
    print(f"Connection: {connection_string}")
    print(f"Test directory: {test_dir}")
    print(f"Table prefix: {table_prefix}")
    
    try:
        # Initialize performance tester
        tester = PerformanceTester(connection_string, test_dir)
        
        # Test database connection
        print("\n🔌 Testing database connection...")
        if not tester.test_database_connection():
            print("❌ Database connection failed!")
            print("Please check:")
            print("1. PostgreSQL is running")
            print("2. Connection string is correct")
            print("3. Database exists")
            print("4. User has proper permissions")
            print(f"Current connection: {connection_string}")
            exit(1)
        else:
            print("✅ Database connection successful")
        
        # Check for existing test tables
        print(f"\n🔍 Checking for existing test tables...")
        existing_tables = tester.check_existing_tables(table_prefix)
        
        if existing_tables:
            print(f"Found {len(existing_tables)} existing test tables:")
            for table in existing_tables[:5]:  # Show first 5
                print(f"  - {table}")
            if len(existing_tables) > 5:
                print(f"  ... and {len(existing_tables) - 5} more")
            
            # Ask user what to do
            response = input("\n❓ Existing test tables found. Options:\n"
                           "1. Clean up existing tables and run fresh tests\n"
                           "2. Keep existing tables and skip cleanup\n"
                           "3. Exit\n"
                           "Enter choice (1-3): ").strip()
            
            if response == "1":
                print("🧹 Cleaning up existing test tables...")
                tester.cleanup_test_tables(existing_tables)
                cleanup_tables = True
            elif response == "2":
                print("📋 Keeping existing tables, will skip cleanup...")
                cleanup_tables = False
            else:
                print("👋 Exiting...")
                exit(0)
        else:
            print("✅ No existing test tables found")
            cleanup_tables = True
        
        # Create realistic test files
        print("\n📁 Creating realistic CNPJ test files...")
        print("This will create structured data similar to actual CNPJ files:")
        for table_type in table_types:
            print(f"  - {table_type}: {', '.join([f'{size}MB' for size in file_sizes])}")
        
        test_files = tester.create_test_files(file_sizes, table_types)
        
        # Show what was created
        print(f"\n📊 Created {len(test_files)} test files:")
        for file_key, file_info in test_files.items():
            table_type = file_info['table_type']
            parquet_size = file_info['parquet_size_mb']
            csv_size = file_info['csv_size_mb']
            rows = file_info['rows']
            print(f"  - {table_type}: {rows:,} rows, Parquet: {parquet_size:.1f}MB, CSV: {csv_size:.1f}MB")
        
        # Run performance tests
        print("\n⚡ Running performance tests...")
        if not tester.duckdb_available:
            print("⚠️  Skipping DuckDB tests (not available)")
            print("   Install with: pip install duckdb")
        else:
            print("✅ DuckDB available - will test all methods")
        
        metrics = tester.run_performance_test(test_files, table_prefix, cleanup_tables)
        
        # Print report
        tester.print_performance_report(metrics)
        
        # Save detailed report
        tester.save_performance_report(metrics, report_file)
        
        print(f"\n✅ Performance testing completed!")
        print(f"📊 Detailed report saved to: {report_file}")
        
        # Show summary of results
        successful_metrics = [m for m in metrics if m.success]
        if successful_metrics:
            print(f"\n📈 SUMMARY:")
            print(f"  - Total tests: {len(metrics)}")
            print(f"  - Successful: {len(successful_metrics)}")
            print(f"  - Failed: {len(metrics) - len(successful_metrics)}")
            
            # Show fastest method overall
            fastest = min(successful_metrics, key=lambda x: x.load_time_seconds)
            print(f"  - Fastest overall: {fastest.method} ({fastest.load_time_seconds:.2f}s)")
        
        if not cleanup_tables:
            print(f"\n💡 Test tables with prefix '{table_prefix}' were preserved for inspection")
            print("   You can examine them in your database to verify data quality")
        
    except Exception as e:
        logger.error(f"Performance testing failed: {e}")
        import traceback
        traceback.print_exc()
        raise 