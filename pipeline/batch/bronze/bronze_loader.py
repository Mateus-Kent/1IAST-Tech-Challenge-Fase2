"""
Bronze Layer Loader
-------------------
Lê os CSVs brutos de data/raw/ e grava em Parquet na camada Bronze.

Regras da camada Bronze:
- Dados preservados exatamente como vieram da fonte
- Nenhuma transformação de negócio
- Apenas adição de metadados de ingestão
"""

import os
import logging
from datetime import datetime, timezone
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
ROOT = Path(__file__).resolve().parents[3]   # raiz do projeto
RAW_DIR = ROOT / "data" / "raw"
BRONZE_DIR = ROOT / "layers" / "bronze"

PIPELINE_VERSION = "1.0.0"

# Mapeamento: nome lógico → arquivo CSV de origem
SOURCES = {
    "meta_brasil":    "meta_alfabetizacao_brasil.csv",
    "meta_municipio": "meta_alfabetizacao_municipio.csv",
    "meta_uf":        "meta_alfabetizacao_uf.csv",
    "indicador_municipio": "indicador_municipio.csv",
    "indicador_uf":   "indicador_uf.csv",
}


# ── Funções ───────────────────────────────────────────────────────────────────

def add_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Adiciona colunas de metadados sem alterar os dados originais."""
    df = df.copy()
    df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = source_file
    df["_pipeline_version"] = PIPELINE_VERSION
    return df


def ingest_source(name: str, filename: str) -> dict:
    """
    Lê um CSV, adiciona metadados e grava em Parquet.
    Retorna um resumo da operação.
    """
    csv_path = RAW_DIR / filename
    parquet_path = BRONZE_DIR / f"{name}.parquet"

    log.info(f"Iniciando ingestão: {name} ← {filename}")

    # Leitura
    df = pd.read_csv(csv_path, dtype=str)   # dtype=str preserva os dados brutos
    rows_raw = len(df)
    log.info(f"  Lidas {rows_raw} linhas de {filename}")

    # Metadados
    df = add_metadata(df, source_file=filename)

    # Gravação em Parquet
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    size_kb = parquet_path.stat().st_size / 1024
    log.info(f"  Gravado: {parquet_path.name} ({size_kb:.1f} KB) — {rows_raw} registros")

    return {
        "fonte": name,
        "arquivo_csv": filename,
        "arquivo_parquet": parquet_path.name,
        "registros": rows_raw,
        "colunas": list(df.columns),
        "tamanho_kb": round(size_kb, 1),
        "status": "ok",
    }


def run():
    log.info("=" * 60)
    log.info("Bronze Layer — início da ingestão batch")
    log.info(f"Versão do pipeline: {PIPELINE_VERSION}")
    log.info("=" * 60)

    resultados = []

    for name, filename in SOURCES.items():
        try:
            resultado = ingest_source(name, filename)
            resultados.append(resultado)
        except FileNotFoundError:
            log.error(f"Arquivo não encontrado: {RAW_DIR / filename}")
            resultados.append({"fonte": name, "status": "erro: arquivo não encontrado"})
        except Exception as e:
            log.error(f"Erro ao processar {name}: {e}")
            resultados.append({"fonte": name, "status": f"erro: {e}"})

    # ── Resumo final ──────────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("RESUMO DA INGESTÃO BRONZE")
    log.info("=" * 60)

    total_registros = 0
    for r in resultados:
        if r["status"] == "ok":
            log.info(f"  ✔ {r['fonte']:<22} {r['registros']:>6} registros  ({r['tamanho_kb']} KB)")
            total_registros += r["registros"]
        else:
            log.warning(f"  ✘ {r['fonte']:<22} {r['status']}")

    log.info("-" * 60)
    log.info(f"  Total ingerido: {total_registros} registros")
    log.info(f"  Destino:        {BRONZE_DIR}")
    log.info("=" * 60)

    return resultados


if __name__ == "__main__":
    run()
