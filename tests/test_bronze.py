"""
Testes — Camada Bronze
-----------------------
Valida que o bronze_loader.py gerou os arquivos Parquet
corretamente com os metadados esperados.
"""

import pytest
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "layers" / "bronze"

SOURCES = [
    "meta_brasil",
    "meta_municipio",
    "meta_uf",
    "indicador_municipio",
    "indicador_uf",
]

METADATA_COLS = ["_ingestion_timestamp", "_source_file", "_pipeline_version"]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def bronze_dfs():
    """Carrega todos os arquivos Parquet da camada Bronze."""
    dfs = {}
    for name in SOURCES:
        path = BRONZE_DIR / f"{name}.parquet"
        if path.exists():
            dfs[name] = pd.read_parquet(path)
    return dfs


# ── Testes ────────────────────────────────────────────────────────────────────

def test_arquivos_gerados():
    """Verifica se todos os arquivos Parquet foram gerados."""
    for name in SOURCES:
        path = BRONZE_DIR / f"{name}.parquet"
        assert path.exists(), f"Arquivo não encontrado: {name}.parquet"


def test_arquivos_nao_vazios(bronze_dfs):
    """Verifica se os arquivos Parquet têm registros."""
    for name, df in bronze_dfs.items():
        assert len(df) > 0, f"{name}.parquet está vazio"


def test_metadados_presentes(bronze_dfs):
    """Verifica se as colunas de metadados foram adicionadas."""
    for name, df in bronze_dfs.items():
        for col in METADATA_COLS:
            assert col in df.columns, f"Coluna '{col}' ausente em {name}.parquet"


def test_source_file_correto(bronze_dfs):
    """Verifica se _source_file aponta para o CSV correto."""
    esperados = {
        "meta_brasil":         "meta_alfabetizacao_brasil.csv",
        "meta_municipio":      "meta_alfabetizacao_municipio.csv",
        "meta_uf":             "meta_alfabetizacao_uf.csv",
        "indicador_municipio": "indicador_municipio.csv",
        "indicador_uf":        "indicador_uf.csv",
    }
    for name, df in bronze_dfs.items():
        valores = df["_source_file"].unique()
        assert len(valores) == 1, f"{name}: múltiplos valores em _source_file"
        assert valores[0] == esperados[name], (
            f"{name}: _source_file incorreto — esperado '{esperados[name]}', "
            f"encontrado '{valores[0]}'"
        )


def test_pipeline_version_presente(bronze_dfs):
    """Verifica se _pipeline_version está preenchido."""
    for name, df in bronze_dfs.items():
        assert df["_pipeline_version"].notna().all(), (
            f"{name}: _pipeline_version tem valores nulos"
        )


def test_volume_minimo(bronze_dfs):
    """Verifica volume mínimo esperado de registros."""
    minimos = {
        "meta_brasil":         1,
        "meta_municipio":      1000,
        "meta_uf":             20,
        "indicador_municipio": 1000,
        "indicador_uf":        50,
    }
    for name, minimo in minimos.items():
        if name in bronze_dfs:
            qtd = len(bronze_dfs[name])
            assert qtd >= minimo, (
                f"{name}: esperado mínimo {minimo} registros, encontrado {qtd}"
            )


def test_sem_colunas_duplicadas(bronze_dfs):
    """Verifica se não há colunas duplicadas nos arquivos."""
    for name, df in bronze_dfs.items():
        cols = list(df.columns)
        assert len(cols) == len(set(cols)), (
            f"{name}: colunas duplicadas encontradas"
        )
