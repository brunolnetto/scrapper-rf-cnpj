# Instruções para Agentes IA - Projeto ETL CNPJ

## Visão Geral do Projeto
Este é um pipeline ETL de produção que baixa, processa e carrega dados públicos de CNPJ da Receita Federal do Brasil no PostgreSQL. O sistema processa ~17GB de dados governamentais descompactados com rastreamento de auditoria e capacidades de carregamento incremental.

## Arquitetura
- **Fluxo ETL Principal**: `src/main.py` → `ETLOrchestrator` → `CNPJ_ETL` → Serviços de Download/Transform/Load
- **Configuração**: Centralizada em `src/setup/config.py` com configurações de banco baseadas em ambiente
- **Banco de Dados**: Padrão dual - BD de auditoria separado do BD principal de produção  
- **Estratégia de Carregamento**: Auto-detecta arquivos Parquet (preferencial) ou CSV usando `UnifiedLoader`
- **Sistema de Auditoria**: Rastreia downloads, tempos de processamento e linhagem de dados em tabelas dedicadas

## Componentes Principais

### Serviço de Configuração (`src/setup/config.py`)
- Fonte única da verdade para todas as configurações via `ConfigurationService`
- Configurações de banco suportam múltiplos ambientes via arquivos `.env`
- Sempre use `config_service.databases['main']` para acessar BD de produção

### Pipeline ETL (`src/core/etl.py`)
- Padrão de inicialização lazy - bancos só conectam quando necessário
- Usa decoradores de propriedade para `@property database` e `@property audit_service`
- Processamento de arquivos segue: Download → Extract → Convert to Parquet → Load to DB

### Carregamento de Dados (`src/core/loading/strategies.py`)
- `DataLoadingStrategy` auto-detecta formato de arquivo (Parquet preferencial sobre CSV)
- `UnifiedLoader` manipula ambos os formatos com mesma interface
- Sempre verifique primeiro por arquivos `.parquet` em `conversion_path`

### Modelos de Banco (`src/database/models.py`)
- **Bases Duplas**: `MainBase` para tabelas de negócio, `AuditBase` para tabelas de auditoria
- **Padrão de Auditoria**: Toda operação cria registros de auditoria com metadados de arquivo
- **Índices**: Tabelas principais têm índices em `cnpj_basico` (chave primária para ligação)

## Padrões de Desenvolvimento

### Acesso à Configuração
```python
# Sempre use ConfigurationService
config_service = ConfigurationService()
db_config = config_service.databases['main']  # Não usar variáveis de ambiente hardcoded
```

### Inicialização de Banco de Dados
```python
# Use padrão lazy loading do etl.py
@property
def database(self):
    if self._database is None:
        self._init_databases()
    return self._database
```

### Processamento de Arquivos
```python
# Verifique Parquet primeiro, fallback para CSV
parquet_file = path_config.conversion_path / f"{table_name}.parquet"
if parquet_file.exists():
    loader.load_parquet_file(table_info, parquet_file)
```

## Comandos de Build & Execução

### Fluxos Principais
- `just run` - Executa ETL para ano/mês atual
- `just run-etl 2024 12` - Executa para ano/mês específico
- `just install` - Instala dependências com uv
- `python -m src.main --year 2024 --month 12 --full-refresh true`

### Desenvolvimento
- Dependências gerenciadas via `uv` (preferencial) ou `pip install -r requirements.txt`
- Linting: `just lint` (usa ruff com detecção de imports/args não utilizados)
- Logs armazenados em `logs/YYYY-MM-DD/HH_MM/` com formato JSON

## Fluxo de Dados & Tabelas

### Fontes de Dados Governamentais
- Downloads de `https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj`
- Tabelas de referência: `cnae`, `quals`, `natju`, `moti`, `pais`, `munic`
- Dados principais: `empresa`, `estabelecimento`, `socios`, `simples`

### Etapas de Processamento de Arquivos
1. **Download**: Arquivos ZIP para `data/DOWNLOAD_FILES/`
2. **Extração**: Arquivos CSV para `data/EXTRACTED_FILES/`
3. **Conversão**: Arquivos Parquet para `data/CONVERTED_FILES/`
4. **Carregamento**: Upsert para PostgreSQL com rastreamento de auditoria

### Ligação entre Tabelas
- Todas as tabelas principais se ligam via coluna `cnpj_basico` (indexada)
- `estabelecimento` estende com `cnpj_ordem` e `cnpj_dv` para CNPJ completo

## Configuração do Ambiente
- Copie `.env_template` para `.env` com credenciais do PostgreSQL
- Variáveis obrigatórias: `POSTGRES_*` (user, password, host, port, database)
- Opcionais: `OUTPUT_PATH`, `EXTRACT_PATH`, `ENVIRONMENT`

## Padrões de Tratamento de Erros
- Use logger de `src/setup/logging.py` com formato JSON
- Serviço de auditoria rastreia falhas e tempos de processamento
- Operações de banco usam rollback de transação em erros
- Operações de arquivo têm lógica de retry com backoff exponencial

## Testes & Exemplos
- Veja `examples/loading_example.py` para padrões de uso do UnifiedLoader
- Notebooks de laboratório em `lab/` para exploração de dados
- Use `data/CONVERTED_FILES/*.parquet` para testes de desenvolvimento
