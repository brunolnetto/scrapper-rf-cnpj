""" 
  - Nome do projeto : ETL - CNPJs da Receita Federal do Brasil
  - Objetivo        : Baixar, transformar e carregar dados da Receita Federal do Brasil
"""
import time

from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL

# Start the timer
start_time = time.time()


# Data folders
download_folder, extract_folder = get_sink_folder()

ano = str(2025)
mes = str(5).zfill(2)

database_name = f"dadosrfb_{ano}{mes}"

# Database setup
database = init_database(database_name)

host_url='https://arquivos.receitafederal.gov.br/dados/cnpj'
data_url = f'{host_url}/dados_abertos_cnpj/{ano}-{mes}'
layout_url=f'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'

# ETL setup
scrapper = CNPJ_ETL(
    database, data_url, layout_url, download_folder, extract_folder, 
    is_parallel=True, delete_zips=True
)

# Scrap data
scrapper.run()

# Stop the timer and calculate the elapsed time
end_time = time.time()
execution_time = end_time - start_time

# Print the time it took to execute the script
print(f"Execution time: {execution_time:.2f} seconds")

