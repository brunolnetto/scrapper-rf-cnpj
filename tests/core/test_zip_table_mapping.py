"""Exact ZIP filename -> table mapping (issue #40)."""

import pytest

from src.core.pipeline import ReceitaCNPJPipeline
from src.core.utils.pipeline import get_zip_to_tablename, table_for_zip_filename

REAL_NAMES = {
    "Cnaes.zip": "cnae",
    "Empresas0.zip": "empresa",
    "Empresas9.zip": "empresa",
    "Estabelecimentos0.zip": "estabelecimento",
    "Estabelecimentos9.zip": "estabelecimento",
    "Motivos.zip": "moti",
    "Municipios.zip": "munic",
    "Naturezas.zip": "natju",
    "Paises.zip": "pais",
    "Qualificacoes.zip": "quals",
    "Simples.zip": "simples",
    "Socios0.zip": "socios",
    "SOCIOS3.ZIP": "socios",
    "Empresas_00.zip": "empresa",
}


@pytest.mark.parametrize(("filename", "table"), REAL_NAMES.items())
def test_real_rfb_names_map_to_their_table(filename, table):
    assert table_for_zip_filename(filename) == table


@pytest.mark.parametrize(
    "filename",
    ["LAYOUT_DADOS_ABERTOS_CNPJ.pdf", "Regime_Tributario.zip", "Estab.zip", "EmpresasBackup.zip", "old_Simples.zip"],
)
def test_other_files_do_not_match(filename):
    assert table_for_zip_filename(filename) is None


def test_get_zip_to_tablename_returns_single_table_lists():
    mapping = get_zip_to_tablename({"Empresas0.zip": [], "LAYOUT.pdf": []})
    assert mapping == {"Empresas0.zip": ["empresa"], "LAYOUT.pdf": []}


def test_pipeline_derivation_uses_exact_mapping():
    pipeline = ReceitaCNPJPipeline.__new__(ReceitaCNPJPipeline)
    assert pipeline._derive_tablename_from_filename("Socios0.zip") == "socios"
    assert pipeline._derive_tablename_from_filename("old_Simples.zip") == "unknown"
