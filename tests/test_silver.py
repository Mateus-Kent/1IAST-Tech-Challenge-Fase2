"""
Testes — Camada Silver
-----------------------
Valida que o silver_loader.py gerou os arquivos Parquet
corretamente com limpeza, tipagem e join das bases.
"""

import pytest
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SILVER_DIR = ROOT / "layers" / "silver"

SOURCES = [
    "meta_brasil",
    "meta_municipio",
    "meta_uf",
    "indicador_municipio",
    "indicador_uf",
    "municipio_consolidado",
    "uf_consolidado",
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def silver_dfs():
    """Carrega todos os arquivos Parquet da camada Silver."""
    dfs = {}
    for name in SOURCES:
        path = SILVER_DIR / f"{name}.parquet"
        if path.exists():
            dfs[name] = pd.read_parquet(path)
    return dfs


# ── Testes ────────────────────────────────────────────────────────────────────

def test_arquivos_gerados():
    """Verifica se todos os arquivos Parquet foram gerados."""
    for name in SOURCES:
        path = SILVER_DIR / f"{name}.parquet"
        assert path.exists(), f"Arquivo não encontrado: {name}.parquet"


def test_metadados_removidos(silver_dfs):
    """Verifica se as colunas de metadados do Bronze foram removidas."""
    meta_cols = ["_ingestion_timestamp", "_source_file", "_pipeline_version"]
    for name, df in silver_dfs.items():
        for col in meta_cols:
            assert col not in df.columns, (
                f"{name}: coluna de metadado '{col}' não foi removida na Silver"
            )


def test_tipos_numericos_indicador_uf(silver_dfs):
    """Verifica se taxa_alfabetizacao foi convertida para float."""
    if "indicador_uf" not in silver_dfs:
        pytest.skip("indicador_uf não encontrado")
    df = silver_dfs["indicador_uf"]
    assert "taxa_alfabetizacao" in df.columns
    assert pd.api.types.is_float_dtype(df["taxa_alfabetizacao"]), (
        "taxa_alfabetizacao não é float em indicador_uf"
    )


def test_tipos_numericos_indicador_municipio(silver_dfs):
    """Verifica se taxa_alfabetizacao foi convertida para float."""
    if "indicador_municipio" not in silver_dfs:
        pytest.skip("indicador_municipio não encontrado")
    df = silver_dfs["indicador_municipio"]
    assert "taxa_alfabetizacao" in df.columns
    assert pd.api.types.is_float_dtype(df["taxa_alfabetizacao"]), (
        "taxa_alfabetizacao não é float em indicador_municipio"
    )


def test_sem_duplicatas_uf_consolidado(silver_dfs):
    """
    Verifica ausência de duplicatas no consolidado de UF.
    A chave considera sigla_uf + ano + rede_x pois existem
    múltiplas linhas por UF/ano (Municipal, Estadual, Privada).
    """
    if "uf_consolidado" not in silver_dfs:
        pytest.skip("uf_consolidado não encontrado")
    df = silver_dfs["uf_consolidado"]
    chave = ["sigla_uf", "ano", "rede_x"]
    cols_existentes = [c for c in chave if c in df.columns]
    duplicatas = df.duplicated(subset=cols_existentes).sum()
    assert duplicatas == 0, (
        f"uf_consolidado: {duplicatas} duplicata(s) encontrada(s)"
    )


def test_sem_duplicatas_municipio_consolidado(silver_dfs):
    """
    Verifica ausência de duplicatas no consolidado de município.
    A chave considera id_municipio + ano + rede_x pois existem
    múltiplas linhas por município/ano (Municipal, Estadual, Privada).
    """
    if "municipio_consolidado" not in silver_dfs:
        pytest.skip("municipio_consolidado não encontrado")
    df = silver_dfs["municipio_consolidado"]
    chave = ["id_municipio", "ano", "rede_x"]
    cols_existentes = [c for c in chave if c in df.columns]
    duplicatas = df.duplicated(subset=cols_existentes).sum()
    assert duplicatas == 0, (
        f"municipio_consolidado: {duplicatas} duplicata(s) encontrada(s)"
    )


def test_join_uf_consolidado(silver_dfs):
    """Verifica se o join gerou as colunas de meta no consolidado de UF."""
    if "uf_consolidado" not in silver_dfs:
        pytest.skip("uf_consolidado não encontrado")
    df = silver_dfs["uf_consolidado"]
    assert "meta_alfabetizacao_2030" in df.columns, (
        "uf_consolidado: coluna meta_alfabetizacao_2030 ausente após join"
    )


def test_join_municipio_consolidado(silver_dfs):
    """Verifica se o join gerou as colunas de meta no consolidado de município."""
    if "municipio_consolidado" not in silver_dfs:
        pytest.skip("municipio_consolidado não encontrado")
    df = silver_dfs["municipio_consolidado"]
    assert "meta_alfabetizacao_2030" in df.columns, (
        "municipio_consolidado: coluna meta_alfabetizacao_2030 ausente após join"
    )


def test_anos_validos_uf_consolidado(silver_dfs):
    """Verifica se os anos estão dentro do intervalo esperado."""
    if "uf_consolidado" not in silver_dfs:
        pytest.skip("uf_consolidado não encontrado")
    df = silver_dfs["uf_consolidado"]
    anos = df["ano"].dropna().unique()
    for ano in anos:
        assert 2019 <= int(ano) <= 2030, (
            f"uf_consolidado: ano inválido encontrado — {ano}"
        )


def test_taxa_alfabetizacao_intervalo(silver_dfs):
    """Verifica se taxa_alfabetizacao está entre 0 e 100."""
    for name in ["indicador_uf", "indicador_municipio"]:
        if name not in silver_dfs:
            continue
        df = silver_dfs[name]
        taxa = df["taxa_alfabetizacao"].dropna()
        assert (taxa >= 0).all() and (taxa <= 100).all(), (
            f"{name}: taxa_alfabetizacao fora do intervalo [0, 100]"
        )
