"""
Testes — Camada Gold
---------------------
Valida que o gold_builder.py gerou os datasets analíticos
corretamente com os campos e volumes esperados.
"""

import pytest
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = ROOT / "layers" / "gold"

DATASETS = [
    "ranking_uf",
    "meta_vs_realizado_uf",
    "evolucao_uf",
    "painel_nacional",
]


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def gold_dfs():
    """Carrega todos os arquivos Parquet da camada Gold."""
    dfs = {}
    for name in DATASETS:
        path = GOLD_DIR / f"{name}.parquet"
        if path.exists():
            dfs[name] = pd.read_parquet(path)
    return dfs


# ── Testes ────────────────────────────────────────────────────────────────────

def test_arquivos_gerados():
    """Verifica se todos os datasets Gold foram gerados."""
    for name in DATASETS:
        path = GOLD_DIR / f"{name}.parquet"
        assert path.exists(), f"Dataset não encontrado: {name}.parquet"


def test_datasets_nao_vazios(gold_dfs):
    """Verifica se os datasets têm registros."""
    for name, df in gold_dfs.items():
        assert len(df) > 0, f"{name}.parquet está vazio"


def test_ranking_uf_colunas(gold_dfs):
    """Verifica se ranking_uf tem as colunas esperadas."""
    if "ranking_uf" not in gold_dfs:
        pytest.skip("ranking_uf não encontrado")
    df = gold_dfs["ranking_uf"]
    colunas_esperadas = [
        "sigla_uf",
        "taxa_alfabetizacao",
        "posicao_ranking",
        "distancia_meta_2030",
        "bateu_meta_2030",
    ]
    for col in colunas_esperadas:
        assert col in df.columns, f"ranking_uf: coluna '{col}' ausente"


def test_ranking_uf_volume(gold_dfs):
    """Verifica se ranking_uf tem no máximo 27 estados."""
    if "ranking_uf" not in gold_dfs:
        pytest.skip("ranking_uf não encontrado")
    df = gold_dfs["ranking_uf"]
    assert len(df) <= 27, (
        f"ranking_uf: esperado máximo 27 estados, encontrado {len(df)}"
    )
    assert len(df) >= 20, (
        f"ranking_uf: esperado mínimo 20 estados, encontrado {len(df)}"
    )


def test_ranking_posicao_sequencial(gold_dfs):
    """Verifica se posicao_ranking é sequencial a partir de 1."""
    if "ranking_uf" not in gold_dfs:
        pytest.skip("ranking_uf não encontrado")
    df = gold_dfs["ranking_uf"].sort_values("posicao_ranking")
    posicoes = list(df["posicao_ranking"])
    esperado = list(range(1, len(df) + 1))
    assert posicoes == esperado, "ranking_uf: posicao_ranking não é sequencial"


def test_meta_vs_realizado_colunas(gold_dfs):
    """Verifica se meta_vs_realizado_uf tem as colunas esperadas."""
    if "meta_vs_realizado_uf" not in gold_dfs:
        pytest.skip("meta_vs_realizado_uf não encontrado")
    df = gold_dfs["meta_vs_realizado_uf"]
    colunas_esperadas = [
        "sigla_uf",
        "ano",
        "taxa_realizada",
        "diferenca_meta",
        "classificacao",
    ]
    for col in colunas_esperadas:
        assert col in df.columns, f"meta_vs_realizado_uf: coluna '{col}' ausente"


def test_meta_vs_realizado_classificacao(gold_dfs):
    """Verifica se classificação tem apenas valores válidos."""
    if "meta_vs_realizado_uf" not in gold_dfs:
        pytest.skip("meta_vs_realizado_uf não encontrado")
    df = gold_dfs["meta_vs_realizado_uf"]
    valores_validos = {
        "Acima da meta",
        "Na meta",
        "Abaixo da meta",
        "Muito abaixo da meta",
        "Sem meta definida",
    }
    valores_encontrados = set(df["classificacao"].dropna().unique())
    invalidos = valores_encontrados - valores_validos
    assert not invalidos, (
        f"meta_vs_realizado_uf: classificações inválidas — {invalidos}"
    )


def test_evolucao_uf_colunas(gold_dfs):
    """Verifica se evolucao_uf tem as colunas esperadas."""
    if "evolucao_uf" not in gold_dfs:
        pytest.skip("evolucao_uf não encontrado")
    df = gold_dfs["evolucao_uf"]
    colunas_esperadas = ["sigla_uf", "taxa_2023", "taxa_2024", "variacao_absoluta"]
    for col in colunas_esperadas:
        assert col in df.columns, f"evolucao_uf: coluna '{col}' ausente"


def test_evolucao_variacao_calculada(gold_dfs):
    """Verifica se variacao_absoluta = taxa_2024 - taxa_2023."""
    if "evolucao_uf" not in gold_dfs:
        pytest.skip("evolucao_uf não encontrado")
    df = gold_dfs["evolucao_uf"].dropna(subset=["taxa_2023", "taxa_2024", "variacao_absoluta"])
    variacao_calculada = (df["taxa_2024"] - df["taxa_2023"]).round(2)
    variacao_salva = df["variacao_absoluta"].round(2)
    assert (variacao_calculada == variacao_salva).all(), (
        "evolucao_uf: variacao_absoluta não confere com taxa_2024 - taxa_2023"
    )


def test_painel_nacional_anos(gold_dfs):
    """Verifica se painel_nacional tem dados para 2023 e 2024."""
    if "painel_nacional" not in gold_dfs:
        pytest.skip("painel_nacional não encontrado")
    df = gold_dfs["painel_nacional"]
    anos = list(df["ano"].astype(int))
    assert 2023 in anos, "painel_nacional: ano 2023 ausente"
    assert 2024 in anos, "painel_nacional: ano 2024 ausente"


def test_painel_nacional_media_valida(gold_dfs):
    """Verifica se taxa_media_nacional está entre 0 e 100."""
    if "painel_nacional" not in gold_dfs:
        pytest.skip("painel_nacional não encontrado")
    df = gold_dfs["painel_nacional"]
    taxa = df["taxa_media_nacional"].dropna()
    assert (taxa >= 0).all() and (taxa <= 100).all(), (
        "painel_nacional: taxa_media_nacional fora do intervalo [0, 100]"
    )
