# Pipeline ETL - Dados Públicos CNPJ

## 📊 Visão Geral

Este projeto implementa um pipeline ETL robusto e escalável para processamento de dados públicos do CNPJ da Receita Federal do Brasil. O sistema é capaz de processar grandes volumes de dados (~17GB descompactados) com auditoria completa e carregamento incremental.

### 🏗️ Modelo de Entidade Relacionamento

![Modelo ERD](https://github.com/brunolnetto/RF_CNPJ/blob/master/images/Dados_RFB_ERD.png)

### ✨ Características Principais

- **ETL de Alta Performance**: Sistema async com processamento paralelo interno
- **Detecção Robusta de Arquivos**: Sistema de 4 camadas com validação de conteúdo
- **Processamento Incremental**: Suporte a carregamento por ano/mês específico
- **Auditoria Completa**: Rastreamento de todas as operações com checksums e metadados
- **Formato Otimizado**: Conversão automática CSV → Parquet para melhor performance
- **Arquitetura Dual**: Bancos separados para produção e auditoria
- **Sistema de Logs**: Logging estruturado em JSON com rotação automática
- **Streaming de Dados**: Processamento de arquivos grandes sem carregar na memória
- **Tolerância a Falhas**: Retry automático com backoff exponencial

### 🔄 Fluxo do Pipeline

1. **Download** - Baixa arquivos ZIP da fonte oficial com retry inteligente
2. **Extração** - Descompacta arquivos CSV com validação de integridade
3. **Conversão** - Transforma dados para formato Parquet otimizado (polars)
4. **Carregamento** - Sistema async de alta performance com upserts em batch

### 🚀 Arquitetura

- **Carregador de Arquivos Aprimorado**: Detecção automática de formato (CSV/Parquet) com validação robusta
- **Processamento Assíncrono**: Processamento paralelo interno com controle de concorrência
- **Estratégia de Carregamento Unificado**: Interface simplificada para todos os tipos de arquivo
- **Pool de Conexões**: Pool de conexões async para máxima performance
- **Eficiente em Memória**: Streaming processing para arquivos de qualquer tamanho

## 📚 Fonte de Dados

- **Dados Oficiais**: [Portal de Dados Abertos do Governo](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- **Layout dos Dados**: [Metadados CNPJ - Receita Federal](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf)
- **Volume**: ~4.7GB compactado | ~17.1GB descompactado
- **Atualização**: Mensal pela Receita Federal

## 🛠️ Requisitos do Sistema

### Infraestrutura
- **Python**: 3.9+ (recomendado 3.11+)
- **PostgreSQL**: 14.2+ 
- **Memória RAM**: Mínimo 8GB (recomendado 16GB+)
- **Espaço em Disco**: ~50GB livres para processamento

### Dependências Python
- **Gerenciador**: [uv](https://github.com/astral-sh/uv) (recomendado) ou pip
- **Principais**: asyncpg (async database), pyarrow (file processing), sqlalchemy (ORM)
- **Performance**: polars (conversão CSV→Parquet apenas)
- **Utilitários**: rich (progress), pydantic (validation), pathlib (paths)
- **Ver**: `requirements.txt` para lista completa

### 📈 Performance
- **Uso de Memória**: ~70% de redução via streaming processing
- **Velocidade de Processamento**: 2-3x mais rápido com paralelismo assíncrono interno  
- **Detecção de Arquivos**: Falhas próximas de zero com validação de 4 camadas
- **Escalabilidade**: Processa 60M+ registros eficientemente

## 🚀 Instalação e Configuração

### 1. Setup do Banco de Dados
```bash
# Conecte ao PostgreSQL
sudo -u postgres psql

# Configure usuário e senha
ALTER USER postgres PASSWORD 'sua_senha_aqui';

# Crie os bancos (produção e auditoria)
CREATE DATABASE dadosrfb;
CREATE DATABASE dadosrfb_analysis;
```

### 2. Configuração do Ambiente
```bash
# Clone o repositório
git clone https://github.com/brunolnetto/scrapper-rf-cnpj.git
cd scrapper-rf-cnpj

# Copie e configure o arquivo de ambiente
cp .env.template .env
# Edite o .env com suas credenciais
```

### 3. Instalação das Dependências
```bash
# Opção 1: Usando uv (recomendado)
pip install uv
uv pip install -r requirements.txt

# Opção 2: Usando pip tradicional
pip install -r requirements.txt
```

### 4. Configuração do `.env`

> 📖 **Documentação Completa**: Consulte [docs/ENVIRONMENT_VARIABLES.md](docs/ENVIRONMENT_VARIABLES.md) para detalhes completos sobre todas as variáveis de ambiente.

```env
# Ambiente
ENVIRONMENT=development

# Banco Principal
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=sua_senha
POSTGRES_DBNAME=dadosrfb

# Banco de Auditoria
AUDIT_DB_HOST=localhost
AUDIT_DB_PORT=5432
AUDIT_DB_USER=postgres
AUDIT_DB_PASSWORD=sua_senha
AUDIT_DB_NAME=dadosrfb_analysis

# Diretórios (opcionais)
OUTPUT_PATH=data/DOWNLOAD_FILES
EXTRACT_PATH=data/EXTRACTED_FILES

# Carregamento
ETL_CHUNK_SIZE=50000
ETL_SUB_BATCH_SIZE=5000
ETL_INTERNAL_CONCURRENCY=3
ETL_ASYNC_POOL_MIN_SIZE=2
ETL_ASYNC_POOL_MAX_SIZE=10
```

### 🔧 Validação da Configuração

Para validar sua configuração antes de executar o ETL:

```bash
# Validar configuração do ambiente
python scripts/validate_env.py

# Guia rápido de referência
cat docs/ENV_QUICK_REFERENCE.md
```

### 🔧 Configurações Avançadas

O sistema suporta configurações avançadas para otimização de performance:

- **`ETL_CHUNK_SIZE`**: Tamanho do batch principal (padrão: 50,000)
- **`ETL_SUB_BATCH_SIZE`**: Tamanho dos sub-batches internos (padrão: 5,000)  
- **`ETL_INTERNAL_CONCURRENCY`**: Paralelismo interno por arquivo (padrão: 3)
- **`ETL_ASYNC_POOL_*`**: Configurações do pool de conexões async

> 💡 **Dica**: Use `python scripts/validate_env.py` para recomendações específicas baseadas nos recursos do seu sistema.

### 📁 Arquivos Suportados

O sistema detecta automaticamente o formato dos arquivos:

- **CSV**: `.csv`, `.txt`, `.dat` - com detecção automática de delimitador
- **Parquet**: `.parquet` - com validação de magic bytes
- **Encoding**: Detecção automática com fallback para encoding específico por tabela
## ▶️ Como Executar

### Comandos Principais (usando just)
```bash
# Executar ETL para mês/ano atual
just run

# Executar para período específico
just run-etl 2024 12

# Ver todos os comandos disponíveis
just help
```

### Execução Manual
```bash
# ETL para período atual
python -m src.main

# ETL para período específico
python -m src.main --year 2024 --month 12

# ETL com refresh completo (limpa tabelas)
python -m src.main --year 2024 --month 12 --full-refresh true

# Limpar tabelas específicas
python -m src.main --clear-tables "empresa,estabelecimento"
```

### Comandos de Desenvolvimento
```bash
# Instalar dependências
just install

# Executar linting
just lint

# Limpar logs e cache
just clean

# Buscar no código
just search "termo_pesquisa"
```

### ⏱️ Tempo de Processamento
- **Dados completos**: 4-8 horas (dependendo da infraestrutura)
- **Processamento incremental**: 30min - 2h
- **Download**: 1-2 horas (dependendo da conexão)
- **Logs**: Disponíveis em `logs/YYYY-MM-DD/HH_MM/`

## 🗃️ Estrutura das Tabelas

Para informações detalhadas, consulte o [layout oficial](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf).

### Tabelas Principais (com índices em `cnpj_basico`)
| Tabela | Descrição | Registros Aprox. |
|--------|-----------|------------------|
| `empresa` | Dados cadastrais da matriz | ~60M |
| `estabelecimento` | Dados por unidade/filial (endereços, telefones) | ~60M |
| `socios` | Dados dos sócios das empresas | ~30M |
| `simples` | Dados de MEI e Simples Nacional | ~40M |

### Tabelas de Referência
| Tabela | Descrição |
|--------|-----------|
| `cnae` | Códigos e descrições de atividades econômicas |
| `quals` | Qualificações de pessoas físicas (sócios, responsáveis) |
| `natju` | Naturezas jurídicas |
| `moti` | Motivos de situação cadastral |
| `pais` | Códigos de países |
| `munic` | Códigos de municípios |

### 🔗 Relacionamentos
- **Chave Principal**: `cnpj_basico` (8 primeiros dígitos do CNPJ)
- **CNPJ Completo**: `cnpj_basico` + `cnpj_ordem` + `cnpj_dv` (em `estabelecimento`)
- **Índices**: Otimizados para consultas por CNPJ e relacionamentos

## 📁 Estrutura do Projeto

```
scrapper-rf-cnpj/
├── src/                   # Código fonte principal
│   ├── main.py            # Ponto de entrada do ETL
│   ├── core/              # Componentes principais do ETL
│   ├── database/          # Modelos e conexões de banco
│   ├── setup/             # Configurações e logging
│   └── utils/             # Utilitários diversos
├── data/                  # Dados processados
│   ├── DOWNLOAD_FILES/    # Arquivos ZIP baixados
│   ├── EXTRACTED_FILES/   # Arquivos CSV extraídos
│   └── CONVERTED_FILES/   # Arquivos Parquet convertidos
├── examples/              # Exemplos de uso
├── lab/                   # Notebooks para análise
├── logs/                  # Logs do sistema
├── justfile               # Comandos automatizados
├── requirements.txt       # Dependências Python
└── .env.template          # Template de configuração
```

## 🔧 Recursos Avançados

### Sistema de Auditoria
- Rastreamento completo de operações
- Metadados de arquivos processados
- Tempos de execução e logs estruturados
- Banco separado para dados de auditoria

### Otimizações de Performance
- Conversão automática para formato Parquet
- Carregamento em lotes (chunking)
- Índices otimizados no PostgreSQL
- Processamento paralelo quando possível

### Monitoramento
- Logs em formato JSON estruturado
- Contadores de registros processados
- Métricas de tempo de execução
- Validação de integridade dos dados

## 🛠️ Desenvolvimento

### Estrutura de Código
- **Configuração Centralizada**: `src/setup/config.py`
- **Padrão Lazy Loading**: Conexões de banco sob demanda
- **Strategy Pattern**: Diferentes estratégias de carregamento
- **Auditoria Integrada**: Rastreamento automático de operações

### Executando Testes
```bash
# Usar exemplos para validação
python examples/loading_example.py

# Análise exploratória
jupyter lab lab/main.ipynb
```

### Contribuindo
1. Fork o projeto
2. Crie uma branch para sua feature
3. Execute os testes e linting
4. Submeta um Pull Request

## 📝 Exemplos de Uso

### Consultas SQL Básicas
```sql
-- Empresas ativas por estado
SELECT uf, COUNT(*) as total_empresas
FROM estabelecimento 
WHERE situacao_cadastral = '02'
GROUP BY uf
ORDER BY total_empresas DESC;

-- CNPJs completos com razão social
SELECT 
    CONCAT(e.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) as cnpj_completo,
    e.razao_social,
    est.nome_fantasia
FROM empresa e
JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico
WHERE est.identificador_matriz_filial = '1';
```

### Análise de Dados
```python
import pandas as pd
from sqlalchemy import create_engine

# Conectar ao banco
engine = create_engine('postgresql://user:pass@localhost/dadosrfb')

# Análise por setor
df = pd.read_sql("""
    SELECT c.descricao as setor, COUNT(*) as empresas
    FROM empresa e
    JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico
    JOIN cnae c ON est.cnae_fiscal_principal = c.codigo
    GROUP BY c.descricao
    ORDER BY empresas DESC
    LIMIT 20
""", engine)
```

## 🆘 Troubleshooting

### Problemas Comuns
- **Erro de conexão**: Verifique credenciais no `.env`
- **Falta de espaço**: Monitore diretório `data/`
- **Timeout de download**: Verifique conexão de internet
- **Memória insuficiente**: Ajuste `chunk_size` na configuração

### Logs e Debugging
- Logs detalhados em `logs/YYYY-MM-DD/HH_MM/`
- Use `just clean` para limpar caches
- Verifique status das tabelas no banco de auditoria

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

## 🤝 Contribuidores

- [Bruno Peixoto](https://github.com/brunolnetto) - Autor principal

## 📧 Contato

Para dúvidas ou sugestões, abra uma [issue](https://github.com/brunolnetto/scrapper-rf-cnpj/issues) no GitHub.

---

⭐ **Se este projeto foi útil, considere dar uma estrela no repositório!**
