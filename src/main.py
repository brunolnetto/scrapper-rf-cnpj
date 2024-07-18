""" 
  - Nome do projeto : ETL - CNPJs da Receita Federal do Brasil
  - Objetivo        : Baixar, transformar e carregar dados da Receita Federal do Brasil
"""

from sqlalchemy import text

from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL

# Data folders
download_folder, extract_folder = get_sink_folder()

# Database setup
database = init_database()

# Data source: You can also access by url: https://dados.rfb.gov.br/CNPJ/
data_url = 'http://200.152.38.155/CNPJ'

# Layout
filename = 'LAYOUT_DADOS_ABERTOS_CNPJ.pdf'
layout_url = f'{data_url}/{filename}'

# ETL setup
scrapper = CNPJ_ETL(
    database, data_url, layout_url, download_folder, extract_folder, 
    is_parallel=False, delete_zips=True
)

# Scrap data
scrapper.run()

