import json
import time
import shutil
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = ROOT / "data" / "streaming_raw"
PROCESSED_DIR = ROOT / "data" / "streaming_processed"
BRONZE_DIR = ROOT / "layers" / "bronze"
BRONZE_STREAMING = BRONZE_DIR / "streaming_eventos.parquet"

PIPELINE_VERSION = "1.0.0"


def setup_directories():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def parse_events(json_files: list[Path]) -> pd.DataFrame:
    """Lê lista de arquivos JSON e retorna DataFrame com metadados de ingestão."""
    records = []
    for filepath in json_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                event = json.load(f)
            event["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            event["_source_file"] = filepath.name
            event["_pipeline_version"] = PIPELINE_VERSION
            records.append(event)
        except json.JSONDecodeError:
            log.error(f"JSON corrompido: {filepath.name} — ignorado")
        except Exception as e:
            log.error(f"Erro ao ler {filepath.name}: {e}")
    return pd.DataFrame(records) if records else pd.DataFrame()


def write_to_bronze(df: pd.DataFrame):
    """Anexa eventos ao Parquet Bronze de streaming (append incremental)."""
    if df.empty:
        return

    if BRONZE_STREAMING.exists():
        existing = pd.read_parquet(BRONZE_STREAMING)
        df = pd.concat([existing, df], ignore_index=True)

    df.to_parquet(BRONZE_STREAMING, index=False, engine="pyarrow")
    log.info(f"  ✔ Bronze atualizado: streaming_eventos.parquet — {len(df)} registros acumulados")


def move_to_processed(json_files: list[Path]):
    for filepath in json_files:
        dest = PROCESSED_DIR / filepath.name
        shutil.move(str(filepath), str(dest))


def process_batch(json_files: list[Path]):
    log.info(f"Lote: {len(json_files)} evento(s) na fila.")

    df = parse_events(json_files)
    if df.empty:
        log.warning("Nenhum evento válido no lote — nada gravado.")
        move_to_processed(json_files)
        return

    for _, row in df.iterrows():
        log.info(
            f"  ⚙ {row.get('event_id')} | mun={row.get('id_municipio')} "
            f"| taxa={row.get('taxa_alfabetizacao_atualizada')}% "
            f"| tipo={row.get('event_type')}"
        )

    write_to_bronze(df)
    move_to_processed(json_files)


def run():
    log.info("=" * 60)
    log.info("Streaming Consumer — ingestão para Bronze")
    log.info("=" * 60)

    setup_directories()
    log.info(f"Vigiando: {RAW_DIR}")
    log.info("Ctrl+C para parar.\n")

    try:
        while True:
            json_files = list(RAW_DIR.glob("*.json"))
            if json_files:
                process_batch(json_files)
            time.sleep(3)
    except KeyboardInterrupt:
        log.info("\nConsumer interrompido.")


if __name__ == "__main__":
    run()
