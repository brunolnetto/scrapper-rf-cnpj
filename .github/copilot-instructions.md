# Instruções para Agentes IA - Projeto ETL CNPJ

## Visão Geral do Projeto
Pipeline ETL de produção que processa dados públicos de CNPJ da Receita Federal do Brasil (~17GB descompactados) no PostgreSQL. Sistema com auditoria completa, carregamento incremental e otimizações de performance baseado em web scraping dinâmico.

## Arquitetura Central
- **Fluxo Principal**: `src/main.py` → `ETLOrchestrator` → `CNPJ_ETL` → Download/Extract/Convert/Load
- **Configuração**: `ConfigurationService` em `src/setup/config.py` - fonte única da verdade
- **Bancos Duplos**: Produção (`MainBase`) + Auditoria (`AuditBase`) com bases SQLAlchemy separadas
- **Auto-detecção**: Parquet preferencial sobre CSV via `UnifiedLoader` com detecção robusta
- **Lazy Loading**: Recursos inicializam sob demanda com pattern `@property`
- **Strategy Pattern**: `DataLoadingStrategy` para carregamento unificado

## Padrões Críticos de Desenvolvimento

### Acesso a Configurações
```python
# SEMPRE use ConfigurationService - nunca env vars direto
config_service = ConfigurationService()
db_config = config_service.databases['main']  # ou 'audit'
etl_config = config_service.etl  # ETL_CHUNK_SIZE, etc.
```

### Inicialização de Recursos (Lazy Loading)
```python
# Padrão obrigatório em CNPJ_ETL - evita conexões desnecessárias
@property
def database(self):
    if self._database is None:
        self._init_databases()
    return self._database

@property 
def data_loader(self):
    if not hasattr(self, '_data_loader') or self._data_loader is None:
        self._data_loader = DataLoadingService(
            self.database, self.config.paths, self.loading_strategy
        )
    return self._data_loader
```

### Carregamento de Dados (Prioridade Parquet)
```python
# Strategy Pattern - sempre verificar Parquet primeiro
parquet_file = path_config.conversion_path / f"{table_name}.parquet"
if parquet_file.exists():
    success, error, rows = loader.load_parquet_file(table_info, parquet_file)
else:
    # fallback para CSV múltiplos
    success, error, rows = loader.load_csv_file(table_info, csv_files)
```

### Detecção Robusta de Arquivos (lab/refactored_fileloader)
```python
# Padrão robusto de 4 camadas para detecção de formato
from lab.refactored_fileloader.src.file_loader import FileLoader

# Detecção automática com validação de conteúdo
loader = FileLoader(file_path)
detected_format = loader.get_format()  # 'csv' ou 'parquet'

# Geração de batches unificada
for batch in loader.batch_generator(headers, chunk_size):
    process_batch(batch)
```

## Comandos Essenciais

### Execução ETL
```bash
# Via just (recomendado) - comando task runner
just run                    # mês/ano atual
just run-etl 2024 12       # período específico

# Execução direta com argumentos CLI  
python -m src.main --year 2024 --month 12 --full-refresh true
python -m src.main --download-only --year 2024 --month 12  # só download
python -m src.main --convert-only  # só conversão CSV→Parquet
```

### Desenvolvimento
```bash
just install    # uv para dependências (package manager moderno)
just lint      # ruff com detecção de imports não utilizados
just clean     # limpar logs e cache
just search "token"  # buscar no código
```

### Dependências e Build
- **Package Manager**: `uv` (moderno, rápido) sobre pip tradicional
- **Linting**: `ruff` com regras F (Pyflakes) + ARG (unused arguments)
- **Python**: >=3.9, configurado via `pyproject.toml`

## Estrutura de Dados Específica

### Tabelas Principais (indexadas por `cnpj_basico`)
- `empresa` (~50M registros) - dados da matriz, capital social transformado
- `estabelecimento` (~60M) - filiais com `cnpj_ordem` + `cnpj_dv`
- `socios` (~30M) - dados dos sócios
- `simples` (~40M) - MEI/Simples Nacional

### Transformações de Dados
```python
# Exemplo de transform_map específico para empresa
def empresa_transform_map(artifact):
    artifact["capital_social"] = artifact["capital_social"].str.replace(",", ".")
    artifact["capital_social"] = artifact["capital_social"].astype(float)
    return artifact
```

### Sistema de Auditoria
- **Rastreamento**: Todo arquivo processado gera registro em `AuditDB`
- **Metadatas**: `AuditService` centraliza criação/inserção de auditorias
- **Validação**: Contagem inicial vs final por tabela

## Fluxo de Processamento

### Pipeline Completo
1. **Scraping**: `scrap_data()` - extrai metadados da fonte gov.br
2. **Download**: Arquivos ZIP para `data/DOWNLOAD_FILES/`
3. **Extração**: CSV para `data/EXTRACTED_FILES/`
4. **Conversão**: Parquet para `data/CONVERTED_FILES/` (otimização)
5. **Carregamento**: Upsert PostgreSQL com auditoria

### Arquitetura de Carregamento Refatorada
- **Detecção Robusta**: `lab/refactored_fileloader` com validação multi-camadas
- **Batch Processing**: Ingestors especializados para CSV/Parquet
- **Paralelismo Interno**: Sub-batches concorrentes dentro do mesmo arquivo
- **Auditoria Avançada**: Manifest com checksums e metadados completos

### Modo Desenvolvimento
- Filtragem por tamanho de arquivo (`development_file_size_limit`)
- Logs estruturados JSON em `logs/YYYY-MM-DD/HH_MM/`
- Configuração via `ENVIRONMENT=development`
- Detecção automática: `config.is_development_mode()`

## Troubleshooting Comum
- **Erro conexão**: Verificar `.env` e dual database setup
- **Timeout download**: Web scraping gov.br pode falhar - retry automático
- **Memória insuficiente**: Ajustar `ETL_CHUNK_SIZE` (padrão 50000)
- **Espaço disco**: Monitorar `data/` (~50GB necessários)
- **Formato inválido**: `UnifiedLoader` detecta robustamente CSV/Parquet
