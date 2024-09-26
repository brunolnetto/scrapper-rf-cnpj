"""
Core constants for the project.
"""

from typing import Callable, Dict, List, Any
from utils.misc import repeat_token

# Constants
FENCE_LENGTH = 35
FENCE_CHARACTER = '#'
FENCE = repeat_token(FENCE_CHARACTER, FENCE_LENGTH)
ENCODING_LATIN1 = 'latin-1'

def empresa_transform_map(artifact: Any) -> Any:
    """
    Transform map for 'empresa' artifact.
    
    Args:
        artifact: The artifact to transform.
        
    Returns:
        The transformed artifact.
    """
    comma_to_period = lambda x: x.replace(',', '.')
    artifact['capital_social'] = artifact['capital_social'].apply(comma_to_period)
    artifact['capital_social'] = artifact['capital_social'].astype(float)
    return artifact

TABLES_INFO_DICT: Dict[str, Dict[str, Any]] = {
    'empresa': {
        'label': 'Empresa',
        'group': 'empresas',
        'columns': [
            'cnpj_basico',
            'razao_social',
            'natureza_juridica',
            'qualificacao_responsavel',
            'capital_social',
            'porte_empresa',
            'ente_federativo_responsavel'
        ],
        'expression': 'EMPRE',
        'transform_map': empresa_transform_map,
        'encoding': ENCODING_LATIN1,
        'index_columns': ['cnpj_basico']
    },
    'estabelecimento': {
        'label': 'Estabelecimento',
        'group': 'estabelecimentos',
        'columns': [
            'cnpj_basico',
            'cnpj_ordem',
            'cnpj_dv',
            'identificador_matriz_filial',
            'nome_fantasia',
            'situacao_cadastral',
            'data_situacao_cadastral',
            'motivo_situacao_cadastral',
            'nome_cidade_exterior',
            'pais',
            'data_inicio_atividade',
            'cnae_fiscal_principal',
            'cnae_fiscal_secundaria',
            'tipo_logradouro',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'municipio',
            'ddd_1',
            'telefone_1',
            'ddd_2',
            'telefone_2',
            'ddd_fax',
            'fax',
            'correio_eletronico',
            'situacao_especial',
            'data_situacao_especial'
        ],
        'expression': 'ESTABELE',
        'encoding': ENCODING_LATIN1,
        'index_columns': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']
    },
    'socios': {
        'label': 'Socios',
        'group': 'socios',
        'columns': [
            'cnpj_basico',
            'identificador_socio',
            'nome_socio_razao_social',
            'cpf_cnpj_socio',
            'qualificacao_socio',
            'data_entrada_sociedade',
            'pais',
            'representante_legal',
            'nome_do_representante',
            'qualificacao_representante_legal',
            'faixa_etaria'
        ],
        'expression': 'SOCIO',
        'encoding': ENCODING_LATIN1,
        'index_columns': ['cnpj_basico']
    },
    'simples': {
        'label': 'Simples',
        'group': 'simples',
        'columns': [
            'cnpj_basico',
            'opcao_pelo_simples',
            'data_opcao_simples',
            'data_exclusao_simples',
            'opcao_mei',
            'data_opcao_mei',
            'data_exclusao_mei'
        ],
        'expression': 'SIMPLES',
        'encoding': ENCODING_LATIN1
    },
    'cnae': {
        'label': 'CNAEs',
        'group': 'cnaes',
        'columns': ['codigo', 'descricao'],
        'expression': 'CNAE',
        'encoding': ENCODING_LATIN1
    },
    'moti': {
        'label': 'Motivos',
        'group': 'motivos',
        'columns': ['codigo', 'descricao'],
        'expression': 'MOTI',
        'encoding': ENCODING_LATIN1
    },
    'munic': {
        'label': 'Municipios',
        'group': 'municipios',
        'columns': ['codigo', 'descricao'],
        'expression': 'MUNIC',
        'encoding': ENCODING_LATIN1
    },
    'natju': {
        'label': 'Naturezas',
        'group': 'naturezas',
        'columns': ['codigo', 'descricao'],
        'expression': 'NATJU',
        'encoding': ENCODING_LATIN1
    },
    'pais': {
        'label': 'Paises',
        'group': 'paises',
        'columns': ['codigo', 'descricao'],
        'expression': 'PAIS',
        'encoding': ENCODING_LATIN1
    },
    'quals': {
        'label': 'Qualificacoes',
        'group': 'qualificacoes',
        'columns': ['codigo', 'descricao'],
        'expression': 'QUALS',
        'encoding': ENCODING_LATIN1
    }
}
