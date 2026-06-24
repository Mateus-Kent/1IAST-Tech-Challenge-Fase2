"""
Gold Layer Builder
------------------
Lê os Parquet tratados da camada Silver e gera datasets
analíticos prontos para dashboards, análises estatísticas
e treinamento de modelos de machine learning.

Datasets gerados:
- indicador_municipio:   Indicador de alfabetização por município (particionado por ano)
- ranking_uf:            Ranking de estados por taxa de alfabetização (2024)
- meta_vs_realizado_uf:  Comparação entre meta e resultado por estado e ano (particionado por ano)
- evolucao_uf:           Evolução da taxa de alfabetização 2023 → 2024
- painel_nacional:       Visão consolidada nacional por ano e rede
"""

import logging
from pathlib import Path

import pandas as pd

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]


load_dotenv(ROOT / ".env.example")


# ── Configuração de logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Caminhos ─────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[3]
# SILVER_DIR = ROOT / "layers" / "silver"
S3_SILVER_DIR = "s3://tech-challenge-fase2-fiap-vitor/layers/silver"

# GOLD_DIR = ROOT / "layers" / "gold"
S3_GOLD_DIR = "s3://tech-challenge-fase2-fiap-vitor/layers/gold"


# Mapeamento de códigos de rede para descrição legível
REDE_MAP = {
    0: "Total",
    1: "Federal",
    2: "Municipal",
    3: "Estadual",
    4: "Federal",
    5: "Privada",
}


# ── Funções auxiliares ────────────────────────────────────────────────────────

def load_silver(name: str) -> pd.DataFrame:
    """Carrega um arquivo Parquet da camada Silver no S3."""
    path = f"{S3_SILVER_DIR}/{name}.parquet"
    try:
        df = pd.read_parquet(path)
    except (FileNotFoundError, OSError) as e:
        raise FileNotFoundError(
            f"Arquivo Silver não encontrado: {path}\n"
            "Execute o silver_loader.py antes de rodar o gold_builder.py"
        ) from e
    log.info(f"Carregado: {name}.parquet — {len(df)} linhas")
    return df


def save_gold(df: pd.DataFrame, name: str, partition_cols: list = None) -> None:
    """Salva dataset analítico em Parquet na camada Gold (S3). Com partition_cols, grava dataset particionado (FinOps)."""
    if partition_cols:
        out_path = f"{S3_GOLD_DIR}/{name}"
        # Converte colunas de partição para int nativo (pyarrow não aceita Int64 nullable)
        df_out = df.copy()
        for col in partition_cols:
            if col in df_out.columns:
                df_out[col] = df_out[col].astype(int)
        df_out.to_parquet(out_path, index=False, engine="pyarrow", partition_cols=partition_cols)
        log.info(f"  ✔ Gravado na AWS: {name}/ particionado por {partition_cols} — {len(df)} registros")
    else:
        path = f"{S3_GOLD_DIR}/{name}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
        log.info(f"  ✔ Gravado na AWS: {name}.parquet — {len(df)} registros")


# ── Datasets analíticos ───────────────────────────────────────────────────────

def build_indicador_municipio(mun_consolidado: pd.DataFrame) -> pd.DataFrame:
    """
    Indicador de alfabetização por município — dataset analítico principal.
    Agrega por município e ano, compara com meta vigente e classifica desempenho.
    Particionado por ano para otimização de queries (FinOps).
    """
    log.info("Construindo: indicador_municipio")

    df = (
        mun_consolidado
        .groupby(["id_municipio", "ano"], as_index=False)
        .agg(
            taxa_alfabetizacao=("taxa_alfabetizacao", "mean"),
            media_portugues=("media_portugues", "mean"),
            taxa_meta_base=("taxa_alfabetizacao_meta_base", "mean"),
            meta_2024=("meta_alfabetizacao_2024", "mean"),
            meta_2025=("meta_alfabetizacao_2025", "mean"),
            meta_2026=("meta_alfabetizacao_2026", "mean"),
            meta_2027=("meta_alfabetizacao_2027", "mean"),
            meta_2028=("meta_alfabetizacao_2028", "mean"),
            meta_2029=("meta_alfabetizacao_2029", "mean"),
            meta_2030=("meta_alfabetizacao_2030", "mean"),
            nivel_alfabetizacao=("nivel_alfabetizacao", "first"),
            percentual_participacao=("percentual_participacao_meta", "mean"),
        )
    )

    df["taxa_alfabetizacao"] = df["taxa_alfabetizacao"].round(2)
    df["media_portugues"] = df["media_portugues"].round(2)

    # meta_{ano} existe apenas para 2024–2030; anos anteriores ficam como NaN
    df["meta_ano_vigente"] = df.apply(
        lambda r: r.get(f"meta_{int(r['ano'])}", None), axis=1
    )
    df["diferenca_meta"] = (df["taxa_alfabetizacao"] - df["meta_ano_vigente"]).round(2)
    df["bateu_meta_2030"] = df["taxa_alfabetizacao"] >= 80.0

    def classificar(row):
        if pd.isna(row["diferenca_meta"]):
            return "Sem meta definida"
        if row["diferenca_meta"] >= 5:
            return "Acima da meta"
        if row["diferenca_meta"] >= 0:
            return "Na meta"
        if row["diferenca_meta"] >= -5:
            return "Abaixo da meta"
        return "Muito abaixo da meta"

    df["classificacao"] = df.apply(classificar, axis=1)

    return df.sort_values(["ano", "id_municipio"]).reset_index(drop=True)


def build_ranking_uf(uf_consolidado: pd.DataFrame) -> pd.DataFrame:
    """
    Ranking de estados por taxa de alfabetização.
    Filtra rede Pública (Municipal + Estadual) e ano 2024.
    Adiciona posição no ranking e distância da meta 2030 (80%).
    """
    log.info("Construindo: ranking_uf")

    META_2030 = 80.0

    df = (
        uf_consolidado[
            (uf_consolidado["ano"] == 2024) &
            (uf_consolidado["rede"].isin([2, 3, 5]))
        ]
        .groupby("sigla_uf", as_index=False)
        .agg(
            taxa_alfabetizacao=("taxa_alfabetizacao", "mean"),
            media_portugues=("media_portugues", "mean"),
            percentual_participacao=("percentual_participacao_meta", "mean"),
        )
        .sort_values("taxa_alfabetizacao", ascending=False)
        .reset_index(drop=True)
    )

    df["posicao_ranking"] = df.index + 1
    df["distancia_meta_2030"] = (META_2030 - df["taxa_alfabetizacao"]).round(2)
    df["bateu_meta_2030"] = df["taxa_alfabetizacao"] >= META_2030
    df["ano_referencia"] = 2024

    # Adiciona meta 2024 para comparação
    if "meta_alfabetizacao_2024" in uf_consolidado.columns:
        meta_ref = (
            uf_consolidado[uf_consolidado["ano"] == 2024]
            .groupby("sigla_uf", as_index=False)["meta_alfabetizacao_2024"]
            .mean()
        )
        df = df.merge(meta_ref, on="sigla_uf", how="left")
        df["bateu_meta_2024"] = df["taxa_alfabetizacao"] >= df["meta_alfabetizacao_2024"]

    return df


def build_meta_vs_realizado_uf(uf_consolidado: pd.DataFrame) -> pd.DataFrame:
    """
    Comparação entre meta e resultado por estado e ano.
    Calcula diferença, percentual de atingimento e classificação.
    """
    log.info("Construindo: meta_vs_realizado_uf")

    df = (
        uf_consolidado
        .groupby(["sigla_uf", "ano"], as_index=False)
        .agg(
            taxa_realizada=("taxa_alfabetizacao", "mean"),
            meta_2024=("meta_alfabetizacao_2024", "mean"),
            meta_2025=("meta_alfabetizacao_2025", "mean"),
            meta_2026=("meta_alfabetizacao_2026", "mean"),
            meta_2027=("meta_alfabetizacao_2027", "mean"),
            meta_2028=("meta_alfabetizacao_2028", "mean"),
            meta_2029=("meta_alfabetizacao_2029", "mean"),
            meta_2030=("meta_alfabetizacao_2030", "mean"),
            percentual_participacao=("percentual_participacao_meta", "mean"),
        )
    )

    # meta_{ano} existe apenas para 2024–2030; anos anteriores ficam como NaN
    df["meta_ano_vigente"] = df.apply(
        lambda r: r.get(f"meta_{int(r['ano'])}", None), axis=1
    )
    df["diferenca_meta"] = (df["taxa_realizada"] - df["meta_ano_vigente"]).round(2)
    df["pct_atingimento_meta"] = (
        (df["taxa_realizada"] / df["meta_ano_vigente"].replace(0, pd.NA)) * 100
    ).round(1)

    # Classificação do desempenho
    def classificar(row):
        if pd.isna(row["diferenca_meta"]):
            return "Sem meta definida"
        if row["diferenca_meta"] >= 5:
            return "Acima da meta"
        if row["diferenca_meta"] >= 0:
            return "Na meta"
        if row["diferenca_meta"] >= -5:
            return "Abaixo da meta"
        return "Muito abaixo da meta"

    df["classificacao"] = df.apply(classificar, axis=1)

    return df.sort_values(["ano", "sigla_uf"]).reset_index(drop=True)


def build_evolucao_uf(uf_consolidado: pd.DataFrame) -> pd.DataFrame:
    """
    Evolução da taxa de alfabetização entre 2023 e 2024.
    Calcula variação absoluta e percentual por estado.
    """
    log.info("Construindo: evolucao_uf")

    df = (
        uf_consolidado
        .groupby(["sigla_uf", "ano"], as_index=False)
        .agg(taxa_alfabetizacao=("taxa_alfabetizacao", "mean"))
    )

    df_pivot = df.pivot(index="sigla_uf", columns="ano", values="taxa_alfabetizacao")
    df_pivot.columns = [f"taxa_{int(c)}" for c in df_pivot.columns]
    df_pivot = df_pivot.reset_index()

    if "taxa_2023" in df_pivot.columns and "taxa_2024" in df_pivot.columns:
        df_pivot["variacao_absoluta"] = (
            df_pivot["taxa_2024"] - df_pivot["taxa_2023"]
        ).round(2)
        df_pivot["variacao_percentual"] = (
            ((df_pivot["taxa_2024"] - df_pivot["taxa_2023"]) / df_pivot["taxa_2023"]) * 100
        ).round(1)
        df_pivot["tendencia"] = df_pivot["variacao_absoluta"].apply(
            lambda v: "Melhorou" if v > 0 else ("Piorou" if v < 0 else "Estável")
        )

    if "variacao_absoluta" in df_pivot.columns:
        return df_pivot.sort_values("variacao_absoluta", ascending=False).reset_index(drop=True)
    return df_pivot.reset_index(drop=True)


def build_painel_nacional(
    uf_consolidado: pd.DataFrame,
    meta_brasil: pd.DataFrame,
) -> pd.DataFrame:
    """
    Visão consolidada nacional por ano.
    Combina média nacional dos indicadores com as metas nacionais.
    """
    log.info("Construindo: painel_nacional")

    media_nacional = (
        uf_consolidado
        .groupby("ano", as_index=False)
        .agg(
            taxa_media_nacional=("taxa_alfabetizacao", "mean"),
            media_portugues_nacional=("media_portugues", "mean"),
            total_ufs=("sigla_uf", "nunique"),
        )
    )
    media_nacional["taxa_media_nacional"] = media_nacional["taxa_media_nacional"].round(2)
    media_nacional["media_portugues_nacional"] = media_nacional["media_portugues_nacional"].round(2)

    # Adiciona metas nacionais se disponíveis
    if not meta_brasil.empty:
        meta_cols = [c for c in meta_brasil.columns if "meta_alfabetizacao" in c]
        meta_ref = meta_brasil[["ano"] + meta_cols].drop_duplicates()
        media_nacional = media_nacional.merge(meta_ref, on="ano", how="left")

    return media_nacional.sort_values("ano").reset_index(drop=True)


# ── Execução principal ────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("Gold Layer — início da construção dos datasets analíticos")
    log.info("=" * 60)

    # Carrega dados da Silver
    uf_consolidado = load_silver("uf_consolidado")
    mun_consolidado = load_silver("municipio_consolidado")
    meta_brasil = load_silver("meta_brasil")

    resultados = []

    # (name, builder, partition_cols)
    datasets = [
        ("indicador_municipio",   lambda: build_indicador_municipio(mun_consolidado),                    ["ano"]),
        ("ranking_uf",            lambda: build_ranking_uf(uf_consolidado),                               None),
        ("meta_vs_realizado_uf",  lambda: build_meta_vs_realizado_uf(uf_consolidado),                    ["ano"]),
        ("evolucao_uf",           lambda: build_evolucao_uf(uf_consolidado),                              None),
        ("painel_nacional",       lambda: build_painel_nacional(uf_consolidado, meta_brasil),             None),
    ]

    for name, builder, partition_cols in datasets:
        try:
            df = builder()
            save_gold(df, name, partition_cols=partition_cols)
            resultados.append({"dataset": name, "registros": len(df), "status": "ok"})
        except Exception as e:
            log.error(f"Erro ao construir {name}: {e}")
            resultados.append({"dataset": name, "status": f"erro: {e}"})

    # ── Resumo ────────────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("RESUMO DA CAMADA GOLD")
    log.info("=" * 60)
    for r in resultados:
        if r["status"] == "ok":
            log.info(f"  ✔ {r['dataset']:<30} {r['registros']:>4} registros")
        else:
            log.warning(f"  ✘ {r['dataset']:<30} {r['status']}")
    log.info("=" * 60)
    log.info("Datasets prontos para dashboards, análises e modelos de IA.")
    log.info("=" * 60)

    return resultados


if __name__ == "__main__":
    run()