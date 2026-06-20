"""
Quality Checker — Validação e Qualidade de Dados
-------------------------------------------------
Valida os arquivos Parquet gerados pela camada Bronze.

Verificações realizadas:
- Duplicidade de registros
- Valores ausentes (nulos)
- Validação de chaves de relacionamento
- Consistência entre tabelas
"""

import logging
from pathlib import Path

import pandas as pd

# ── Configuração de logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Caminhos ─────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]   # raiz do projeto
BRONZE_DIR = ROOT / "layers" / "bronze"

# Fontes esperadas na camada Bronze
SOURCES = [
    "meta_brasil",
    "meta_municipio",
    "meta_uf",
    "indicador_municipio",
    "indicador_uf",
]

# Colunas de metadados adicionadas pelo bronze_loader (ignorar em algumas validações)
METADATA_COLS = ["_ingestion_timestamp", "_source_file", "_pipeline_version"]


# ── Funções de validação ──────────────────────────────────────────────────────

def load_parquet(name: str) -> pd.DataFrame | None:
    """Carrega um arquivo Parquet da camada Bronze."""
    path = BRONZE_DIR / f"{name}.parquet"
    if not path.exists():
        log.error(f"Arquivo não encontrado: {path}")
        return None
    df = pd.read_parquet(path)
    log.info(f"Carregado: {name}.parquet — {len(df)} linhas, {len(df.columns)} colunas")
    return df


def check_duplicates(df: pd.DataFrame, name: str) -> dict:
    """Verifica registros duplicados (excluindo colunas de metadados)."""
    cols = [c for c in df.columns if c not in METADATA_COLS]
    duplicados = df.duplicated(subset=cols).sum()

    status = "ok" if duplicados == 0 else "alerta"
    log.info(f"  [Duplicatas] {name}: {duplicados} duplicata(s) encontrada(s) — {status}")

    return {
        "verificacao": "duplicatas",
        "fonte": name,
        "duplicatas": int(duplicados),
        "status": status,
    }


def check_nulls(df: pd.DataFrame, name: str, threshold_pct: float = 20.0) -> dict:
    """
    Verifica valores nulos por coluna.
    Alerta se alguma coluna tiver mais de threshold_pct% de nulos.
    """
    cols = [c for c in df.columns if c not in METADATA_COLS]
    total = len(df)
    alertas = []

    for col in cols:
        nulos = df[col].isnull().sum()
        pct = (nulos / total * 100) if total > 0 else 0
        if pct > threshold_pct:
            alertas.append({"coluna": col, "nulos": int(nulos), "pct": round(pct, 1)})
            log.warning(f"  [Nulos] {name}.{col}: {nulos} nulos ({pct:.1f}%) — acima do limite de {threshold_pct}%")

    status = "ok" if not alertas else "alerta"
    if status == "ok":
        log.info(f"  [Nulos] {name}: nenhuma coluna acima do limite de {threshold_pct}% — ok")

    return {
        "verificacao": "nulos",
        "fonte": name,
        "colunas_com_alerta": alertas,
        "status": status,
    }


def check_relationship(
    df_left: pd.DataFrame,
    name_left: str,
    col_left: str,
    df_right: pd.DataFrame,
    name_right: str,
    col_right: str,
) -> dict:
    """
    Valida se os valores de col_left existem em col_right (integridade referencial).
    Útil para verificar se municípios/UFs nos indicadores existem nas metas.
    """
    if df_left is None or df_right is None:
        return {"verificacao": "relacionamento", "status": "ignorado — arquivo ausente"}

    if col_left not in df_left.columns or col_right not in df_right.columns:
        log.warning(f"  [Relacionamento] Coluna não encontrada: {col_left} ou {col_right}")
        return {
            "verificacao": "relacionamento",
            "status": f"ignorado — coluna '{col_left}' ou '{col_right}' não encontrada",
        }

    valores_left = set(df_left[col_left].dropna().unique())
    valores_right = set(df_right[col_right].dropna().unique())
    orfaos = valores_left - valores_right

    status = "ok" if not orfaos else "alerta"
    log.info(
        f"  [Relacionamento] {name_left}.{col_left} → {name_right}.{col_right}: "
        f"{len(orfaos)} valor(es) órfão(s) — {status}"
    )
    if orfaos:
        log.warning(f"    Exemplos: {list(orfaos)[:5]}")

    return {
        "verificacao": "relacionamento",
        "de": f"{name_left}.{col_left}",
        "para": f"{name_right}.{col_right}",
        "orfaos": len(orfaos),
        "exemplos": list(orfaos)[:5],
        "status": status,
    }


def check_consistency(df: pd.DataFrame, name: str) -> dict:
    """
    Verifica consistência básica dos dados:
    - Valores negativos em colunas numéricas
    - Percentuais fora do intervalo [0, 100]
    """
    cols = [c for c in df.columns if c not in METADATA_COLS]
    alertas = []

    for col in cols:
        # Tenta converter para numérico para verificar consistência
        serie = pd.to_numeric(df[col], errors="coerce")
        if serie.isna().all():
            continue  # coluna não é numérica

        negativos = (serie < 0).sum()
        if negativos > 0:
            alertas.append({"coluna": col, "problema": "valores negativos", "quantidade": int(negativos)})
            log.warning(f"  [Consistência] {name}.{col}: {negativos} valor(es) negativo(s)")

        # Heurística: colunas com "pct", "percentual", "meta", "indicador" devem ser 0-100
        # Só checa > 100 aqui; negativos já foram capturados acima
        col_lower = col.lower()
        if any(k in col_lower for k in ["pct", "percentual", "meta", "indicador", "taxa"]):
            acima = (serie > 100).sum()
            if acima > 0:
                alertas.append({"coluna": col, "problema": "acima de 100", "quantidade": int(acima)})
                log.warning(f"  [Consistência] {name}.{col}: {acima} valor(es) acima de 100")

    status = "ok" if not alertas else "alerta"
    if status == "ok":
        log.info(f"  [Consistência] {name}: sem problemas encontrados — ok")

    return {
        "verificacao": "consistencia",
        "fonte": name,
        "alertas": alertas,
        "status": status,
    }


# ── Execução principal ────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("Quality Checker — início da validação")
    log.info("=" * 60)

    # Carrega todos os arquivos Bronze
    dfs = {name: load_parquet(name) for name in SOURCES}

    resultados = []

    # ── Validações por fonte ──────────────────────────────────────────────────
    for name, df in dfs.items():
        if df is None:
            resultados.append({"fonte": name, "status": "erro: arquivo não encontrado"})
            continue

        log.info(f"\n── Validando: {name} ──")
        resultados.append(check_duplicates(df, name))
        resultados.append(check_nulls(df, name))
        resultados.append(check_consistency(df, name))

    # ── Validações de relacionamento entre tabelas ────────────────────────────
    log.info("\n── Validando relacionamentos entre tabelas ──")

    # Indicador por UF deve ter UFs que existem nas metas por UF
    resultados.append(check_relationship(
        dfs.get("indicador_uf"), "indicador_uf", "sigla_uf",
        dfs.get("meta_uf"),     "meta_uf",      "sigla_uf",
    ))

    # Indicador por município deve ter municípios que existem nas metas por município
    resultados.append(check_relationship(
        dfs.get("indicador_municipio"), "indicador_municipio", "id_municipio",
        dfs.get("meta_municipio"),      "meta_municipio",      "id_municipio",
    ))

    # ── Resumo final ──────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("RESUMO DA VALIDAÇÃO")
    log.info("=" * 60)

    total_ok = sum(1 for r in resultados if r.get("status") == "ok")
    total_alerta = sum(1 for r in resultados if r.get("status") == "alerta")
    total_erro = sum(1 for r in resultados if "erro" in str(r.get("status", "")))

    log.info(f"  ✔ OK:      {total_ok}")
    log.info(f"  ⚠ Alertas: {total_alerta}")
    log.info(f"  ✘ Erros:   {total_erro}")
    log.info("=" * 60)

    return resultados


if __name__ == "__main__":
    run()