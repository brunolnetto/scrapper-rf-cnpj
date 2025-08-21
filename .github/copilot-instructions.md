# Instruções para Agentes IA - Projeto ETL CNPJ

## Visão Geral do Projeto
Pipeline ETL de produção que baixa, processa e carrega dados públicos de CNPJ da Receita Federal do Brasil (~17GB descompactados) no PostgreSQL. Sistema com auditoria completa, carregamento incremental e otimizações de performance.

## Arquitetura Central
- **Fluxo Principal**: `src/main.py` → `ETLOrchestrator` → `CNPJ_ETL` → Download/Extract/Convert/Load
- **Configuração**: `ConfigurationService` em `src/setup/config.py` - fonte única da verdade
- **Bancos Duplos**: Produção (`main`) + Auditoria (`audit`) com bases SQLAlchemy separadas
- **Auto-detecção**: Parquet preferencial sobre CSV via `UnifiedLoader`
- **Lazy Loading**: Conexões de banco inicializam sob demanda com `@property`

## Padrões Críticos de Desenvolvimento

### Acesso a Configurações
```python
# SEMPRE use ConfigurationService - nunca env vars direto
config_service = ConfigurationService()
db_config = config_service.databases['main']  # ou 'audit'
```

### Inicialização de Recursos
```python
# Padrão lazy loading obrigatório em CNPJ_ETL
@property
def database(self):
    if self._database is None:
        self._init_databases()
    return self._database
```

### Carregamento de Dados
```python
# Sempre verificar Parquet primeiro
parquet_file = path_config.conversion_path / f"{table_name}.parquet"
if parquet_file.exists():
    loader.load_parquet_file(table_info, parquet_file)
else:
    # fallback para CSV
```

## Comandos Essenciais

### Execução ETL
```bash
# Via just (recomendado)
just run                    # mês/ano atual
just run-etl 2024 12       # período específico

# Execução direta
python -m src.main --year 2024 --month 12 --full-refresh true
```

### Desenvolvimento
```bash
just install    # uv para dependências
just lint      # ruff com detecção de imports não utilizados
just clean     # limpar logs e cache
```

## Estrutura de Dados Específica

### Tabelas Principais (indexadas por `cnpj_basico`)
- `empresa` (~50M registros) - dados da matriz
- `estabelecimento` (~60M) - filiais com `cnpj_ordem` + `cnpj_dv`
- `socios` (~30M) - dados dos sócios
- `simples` (~40M) - MEI/Simples Nacional

### Sistema de Auditoria
- **Rastreamento**: Todo arquivo processado gera registro em `AuditDB`
- **Metadados**: Timestamps, tamanhos, contadores de registros
- **Validação**: Contagem inicial vs final por tabela

## Fluxo de Processamento

### Pipeline Completo
1. **Scraping**: `scrap_data()` - extrai metadados da fonte gov.br
2. **Download**: Arquivos ZIP para `data/DOWNLOAD_FILES/`
3. **Extração**: CSV para `data/EXTRACTED_FILES/`
4. **Conversão**: Parquet para `data/CONVERTED_FILES/` (otimização)
5. **Carregamento**: Upsert PostgreSQL com auditoria

### Modo Desenvolvimento
- Filtragem por tamanho de arquivo (`development_file_size_limit`)
- Logs estruturados JSON em `logs/YYYY-MM-DD/HH_MM/`
- Configuração via `ENVIRONMENT=development`
### Modo Desenvolvimento
- Filtragem por tamanho de arquivo (`development_file_size_limit`)
- Logs estruturados JSON em `logs/YYYY-MM-DD/HH_MM/`
- Configuração via `ENVIRONMENT=development`

## Troubleshooting Comum
- **Erro conexão**: Verificar `.env` e dual database setup
- **Timeout download**: Web scraping gov.br pode falhar - retry automático
- **Memória insuficiente**: Ajustar `ETL_CHUNK_SIZE` (padrão 50000)
- **Espaço disco**: Monitorar `data/` (~50GB necessários)
