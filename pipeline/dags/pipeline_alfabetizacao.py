"""
DAG — Pipeline de Alfabetização
--------------------------------
Orquestra as camadas Bronze → Silver → Gold do pipeline batch.
Executa diariamente; cada task é independente e pode ser reexecutada.

Ordem de execução:
  bronze_ingestion >> silver_transform >> gold_build >> quality_check
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Adiciona raiz do projeto ao path para imports das camadas
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "engenharia-dados",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def task_bronze():
    from pipeline.batch.bronze.bronze_loader import run
    log.info("Iniciando ingestão Bronze...")
    run()


def task_silver():
    from pipeline.batch.silver.silver_loader import run
    log.info("Iniciando transformação Silver...")
    run()


def task_gold():
    from pipeline.batch.gold.gold_builder import run
    log.info("Iniciando construção Gold...")
    run()


def task_quality():
    from validation.quality_checks.quality_checker import run
    log.info("Iniciando validação de qualidade...")
    resultados = run()
    erros = [r for r in resultados if "erro" in str(r.get("status", ""))]
    if erros:
        raise ValueError(f"Quality check falhou: {erros}")


with DAG(
    dag_id="pipeline_alfabetizacao",
    description="Pipeline Medalhão: Bronze → Silver → Gold para dados de alfabetização",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["alfabetizacao", "medalhao", "batch"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=task_bronze,
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=task_silver,
    )

    gold = PythonOperator(
        task_id="gold_build",
        python_callable=task_gold,
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality,
    )

    bronze >> silver >> gold >> quality
