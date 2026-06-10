import logging
from pathlib import Path
import pandas as pd

# Configuração de Log idêntica à da Bronze para manter o padrão
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Configuração de Diretórios
ROOT = Path(__file__).resolve().parents[3]
BRONZE_DIR = ROOT / "layers" / "bronze"
SILVER_DIR = ROOT / "layers" / "silver"

META_COLS = ["_ingestion_timestamp", "_source_file", "_pipeline_version"]

# Colunas de métricas tem que colocar em float
META_FLOAT_COLS = [
    "taxa_alfabetizacao", "percentual_participacao",
    "meta_alfabetizacao_2024", "meta_alfabetizacao_2025", "meta_alfabetizacao_2026",
    "meta_alfabetizacao_2027", "meta_alfabetizacao_2028", "meta_alfabetizacao_2029",
    "meta_alfabetizacao_2030",
]

# ==========================================
# FUNÇÕES DE TRANSFORMAÇÃO BÁSICAS
# ==========================================

def drop_meta(df: pd.DataFrame) -> pd.DataFrame:
    """Remove as colunas de metadados da camada Bronze."""
    return df.drop(columns=[c for c in META_COLS if c in df.columns])

def cast_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Converte colunas para float64."""
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def cast_int(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Converte colunas para Int64 (aceita NaN)."""
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

# ==========================================
# REGRAS DE QUALIDADE DE DADOS (EDITAL)
# ==========================================

def validate_quality(df: pd.DataFrame, table_name: str, key_columns: list = None):
    """
    Aplica validações de qualidade. Se falhar, o pipeline quebra (Fail Fast).
    """
    # Verificação de duplicidade na tabela inteira
    duplicatas = df.duplicated().sum()
    assert duplicatas == 0, f"Falha de Qualidade: {table_name} possui {duplicatas} linhas duplicadas!"
    
    # Validação de chaves (se fornecidas)
    if key_columns:
        for col in key_columns:
            nulos = df[col].isnull().sum()
            assert nulos == 0, f"Falha de Qualidade: Chave primária '{col}' em {table_name} possui {nulos} valores nulos!"

# ==========================================
# PROCESSAMENTO DAS TABELAS
# ==========================================

def process_indicadores():
    log.info("Processando Indicadores...")
    ind_mun = pd.read_parquet(BRONZE_DIR / "indicador_municipio.parquet")
    ind_uf = pd.read_parquet(BRONZE_DIR / "indicador_uf.parquet")

    nivel_cols = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
    float_cols = ["taxa_alfabetizacao", "media_portugues"] + nivel_cols

    # Tratamento Município
    s_ind_mun = (
        ind_mun.pipe(drop_meta)
        .pipe(cast_int, ["ano", "serie", "rede"])
        .pipe(cast_numeric, float_cols)
    )
    validate_quality(s_ind_mun, "indicador_municipio", ["id_municipio", "ano"])

    # Tratamento UF
    s_ind_uf = (
        ind_uf.pipe(drop_meta)
        .pipe(cast_int, ["ano", "serie", "rede"])
        .pipe(cast_numeric, float_cols)
    )
    validate_quality(s_ind_uf, "indicador_uf", ["sigla_uf", "ano"])

    return s_ind_mun, s_ind_uf


def process_metas():
    log.info("Processando Metas...")
    meta_br = pd.read_parquet(BRONZE_DIR / "meta_brasil.parquet")
    meta_mun = pd.read_parquet(BRONZE_DIR / "meta_municipio.parquet")
    meta_uf = pd.read_parquet(BRONZE_DIR / "meta_uf.parquet")

    s_meta_br = (
        meta_br.pipe(drop_meta)
        .pipe(cast_int, ["ano", "rede"])
        .pipe(cast_numeric, META_FLOAT_COLS)
    )
    validate_quality(s_meta_br, "meta_brasil", ["ano"])

    s_meta_mun = (
        meta_mun.pipe(drop_meta)
        .pipe(cast_int, ["ano", "rede"])
        .pipe(cast_numeric, META_FLOAT_COLS)
    )
    validate_quality(s_meta_mun, "meta_municipio", ["id_municipio", "ano"])

    s_meta_uf = (
        meta_uf.pipe(drop_meta)
        .pipe(cast_int, ["ano", "rede"])
        .pipe(cast_numeric, META_FLOAT_COLS)
    )
    validate_quality(s_meta_uf, "meta_uf", ["sigla_uf", "ano"])

    return s_meta_br, s_meta_mun, s_meta_uf


def process_consolidacao(s_ind_mun, s_ind_uf, s_meta_mun, s_meta_uf):
    log.info("Realizando Integração (Join) das bases...")
    
    # Rename das metas para evitar conflito de colunas no join
    meta_mun_renamed = s_meta_mun.rename(columns={
        "taxa_alfabetizacao": "taxa_alfabetizacao_meta_base",
        "percentual_participacao": "percentual_participacao_meta",
    })
    meta_uf_renamed = s_meta_uf.rename(columns={
        "taxa_alfabetizacao": "taxa_alfabetizacao_meta_base",
        "percentual_participacao": "percentual_participacao_meta",
    })

    # Join Município
    mun_consolidado = s_ind_mun.merge(
        meta_mun_renamed,
        on=["id_municipio", "ano"], # ATENÇÃO: Considere adicionar 'rede' aqui se padronizar a coluna no futuro
        how="left",
    )
    validate_quality(mun_consolidado, "municipio_consolidado", ["id_municipio", "ano"])

    # Join UF
    uf_consolidado = s_ind_uf.merge(
        meta_uf_renamed,
        on=["sigla_uf", "ano"],
        how="left",
    )
    validate_quality(uf_consolidado, "uf_consolidado", ["sigla_uf", "ano"])

    return mun_consolidado, uf_consolidado


def save_silver(df_dict):
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in df_dict.items():
        path = SILVER_DIR / f"{name}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
        size_kb = path.stat().st_size / 1024
        log.info(f"  ✔ Gravado: {name}.parquet ({size_kb:.1f} KB) — {df.shape[0]} registros")


def run():
    log.info("=" * 60)
    log.info("Silver Layer — Limpeza, Tipagem e Integração")
    log.info("=" * 60)

    try:
        s_ind_mun, s_ind_uf = process_indicadores()
        s_meta_br, s_meta_mun, s_meta_uf = process_metas()
        mun_consolidado, uf_consolidado = process_consolidacao(s_ind_mun, s_ind_uf, s_meta_mun, s_meta_uf)

        outputs = {
            "indicador_municipio": s_ind_mun,
            "indicador_uf": s_ind_uf,
            "meta_brasil": s_meta_br,
            "meta_municipio": s_meta_mun,
            "meta_uf": s_meta_uf,
            "municipio_consolidado": mun_consolidado,
            "uf_consolidado": uf_consolidado,
        }

        save_silver(outputs)
        log.info("=" * 60)
        log.info("Ingestão Silver concluída com sucesso!")
        log.info("=" * 60)

    except AssertionError as ae:
        log.error(str(ae))
        log.error("Pipeline abortado devido a falha de qualidade de dados.")
    except Exception as e:
        log.error(f"Erro inesperado no pipeline Silver: {e}")

if __name__ == "__main__":
    run()