""" 
  - Nome do projeto : ETL - CNPJs da Receita Federal do Brasil
  - Objetivo        : Baixar, transformar e carregar dados da Receita Federal do Brasil
"""
from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL

# Data folders
download_folder, extract_folder = get_sink_folder()

# Database setup
database = init_database()

# Data source: You can also access by url: https://dados.rfb.gov.br/CNPJ/dados_abertos_cnpj
ano = str(2024)
mes = str(10).zfill(2)

host_url='https://arquivos.receitafederal.gov.br/dados/cnpj'
data_url = f'{host_url}/dados_abertos_cnpj/{ano}-{mes}'
layout_url=f'{host_url}/LAYOUT_DADOS_ABERTOS_CNPJ.pdf'

# ETL setup
scrapper = CNPJ_ETL(
    database, data_url, layout_url, download_folder, extract_folder, 
    is_parallel=True, delete_zips=False
)

# Scrap data
scrapper.create_indices()



