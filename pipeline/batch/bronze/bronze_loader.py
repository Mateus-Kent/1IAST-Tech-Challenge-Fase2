import logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]

load_dotenv(ROOT / ".env.example")

S3_BRONZE_DIR = "s3://tech-challenge-fase2-fiap-vitor/layers/bronze"

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = ROOT / "data" / "raw"
BRONZE_DIR = ROOT / "layers" / "bronze"

PIPELINE_VERSION = "1.0.0"

SOURCES = {
    "meta_brasil":        "meta_alfabetizacao_brasil.csv",
    "meta_municipio":     "meta_alfabetizacao_municipio.csv",
    "meta_uf":            "meta_alfabetizacao_uf.csv",
    "indicador_municipio": "indicador_municipio.csv",
    "indicador_uf":       "indicador_uf.csv",
}


def add_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    df = df.copy()
    df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = source_file
    df["_pipeline_version"] = PIPELINE_VERSION
    return df


##Antigo sem aws
# def ingest_source(name: str, filename: str) -> dict:
#     csv_path = RAW_DIR / filename
#     parquet_path = BRONZE_DIR / f"{name}.parquet"

#     log.info(f"Iniciando ingestão: {name} ← {filename}")

#     # dtype=str preserva valores brutos; conversões acontecem na Silver
#     df = pd.read_csv(csv_path, dtype=str)
#     rows_raw = len(df)
#     log.info(f"  Lidas {rows_raw} linhas de {filename}")

#     df = add_metadata(df, source_file=filename)

#     BRONZE_DIR.mkdir(parents=True, exist_ok=True)
#     df.to_parquet(parquet_path, index=False, engine="pyarrow")

#     size_kb = parquet_path.stat().st_size / 1024
#     log.info(f"  Gravado: {parquet_path.name} ({size_kb:.1f} KB) — {rows_raw} registros")

#     return {
#         "fonte": name,
#         "arquivo_csv": filename,
#         "arquivo_parquet": parquet_path.name,
#         "registros": rows_raw,
#         "colunas": list(df.columns),
#         "tamanho_kb": round(size_kb, 1),
#         "status": "ok",
#     }

def ingest_source(name: str, filename: str) -> dict:
    csv_path = RAW_DIR / filename
    parquet_path = f"{S3_BRONZE_DIR}/{name}.parquet" # Caminho S3

    log.info(f"Iniciando ingestão: {name} ← {filename}")
    df = pd.read_csv(csv_path, dtype=str)
    rows_raw = len(df)

    df = add_metadata(df, source_file=filename)

    # Salva direto na nuvem!
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    log.info(f"  ✔ Gravado na AWS: {name}.parquet — {rows_raw} registros")

    # Como tiramos o tamanho_kb, ajuste o return para evitar erro:
    return {
        "fonte": name,
        "arquivo_csv": filename,
        "arquivo_parquet": f"{name}.parquet",
        "registros": rows_raw,
        "status": "ok",
    }

def run():
    log.info("=" * 60)
    log.info(f"Bronze Layer — ingestão batch  (v{PIPELINE_VERSION})")
    log.info("=" * 60)

    resultados = []

    for name, filename in SOURCES.items():
        try:
            resultados.append(ingest_source(name, filename))
        except FileNotFoundError:
            log.error(f"Arquivo não encontrado: {RAW_DIR / filename}")
            resultados.append({"fonte": name, "status": "erro: arquivo não encontrado"})
        except Exception as e:
            log.error(f"Erro ao processar {name}: {e}")
            resultados.append({"fonte": name, "status": f"erro: {e}"})

    log.info("")
    log.info("=" * 60)
    log.info("RESUMO DA INGESTÃO BRONZE")
    log.info("=" * 60)

    total_registros = 0
    for r in resultados:
        if r["status"] == "ok":
            log.info(f"  ✔ {r['fonte']:<22} {r['registros']:>6} registros")
            total_registros += r["registros"]
        else:
            log.warning(f"  ✘ {r['fonte']:<22} {r['status']}")

    log.info("-" * 60)
    log.info(f"  Total ingerido: {total_registros} registros")
    log.info(f"  Destino:        {S3_BRONZE_DIR}")
    log.info("=" * 60)

    return resultados


if __name__ == "__main__":
    run()