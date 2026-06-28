"""
Pipeline Monitor — Monitoramento e Alertas
------------------------------------------
Monitora a saúde do pipeline verificando:
- Volume de dados por camada
- Latência de execução
- Integridade dos arquivos gerados
- Alertas de anomalias

Execute após rodar o pipeline completo para um relatório
de saúde detalhado.
"""

import logging
import time
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
ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = ROOT / "layers" / "bronze"
SILVER_DIR = ROOT / "layers" / "silver"
GOLD_DIR   = ROOT / "layers" / "gold"

# ── Arquivos esperados por camada ─────────────────────────────────────────────
BRONZE_ESPERADOS = [
    "meta_brasil",
    "meta_municipio",
    "meta_uf",
    "indicador_municipio",
    "indicador_uf",
]

SILVER_ESPERADOS = [
    "meta_brasil",
    "meta_municipio",
    "meta_uf",
    "indicador_municipio",
    "indicador_uf",
    "municipio_consolidado",
    "uf_consolidado",
]

GOLD_ESPERADOS = [
    "ranking_uf",
    "meta_vs_realizado_uf",
    "evolucao_uf",
    "painel_nacional",
]

# Limites de alerta
ALERTA_VOLUME_MINIMO = {
    "bronze": {"meta_brasil": 1, "meta_municipio": 1000, "meta_uf": 20,
               "indicador_municipio": 1000, "indicador_uf": 50},
    "silver": {"uf_consolidado": 50, "municipio_consolidado": 1000},
    "gold":   {"ranking_uf": 20, "meta_vs_realizado_uf": 20,
               "evolucao_uf": 20, "painel_nacional": 1},
}

ALERTA_TAMANHO_MINIMO_KB = 1.0


# ── Funções de monitoramento ──────────────────────────────────────────────────

def checar_camada(nome_camada: str, diretorio: Path, arquivos: list) -> dict:
    """
    Verifica integridade, volume e tamanho dos arquivos de uma camada.
    Retorna um relatório com status e métricas.
    """
    log.info(f"\n── Monitorando camada: {nome_camada.upper()} ──")

    relatorio = {
        "camada": nome_camada,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "arquivos": [],
        "total_registros": 0,
        "total_tamanho_kb": 0,
        "alertas": [],
        "status": "ok",
    }

    for nome in arquivos:
        path = diretorio / f"{nome}.parquet"

        # Verifica existência
        if not path.exists():
            msg = f"{nome}.parquet não encontrado em {nome_camada}"
            log.error(f"  ✘ {msg}")
            relatorio["alertas"].append({"tipo": "arquivo_ausente", "detalhe": msg})
            relatorio["status"] = "erro"
            continue

        # Lê o arquivo
        try:
            inicio = time.time()
            df = pd.read_parquet(path)
            latencia_ms = round((time.time() - inicio) * 1000, 1)
        except Exception as e:
            msg = f"Erro ao ler {nome}.parquet: {e}"
            log.error(f"  ✘ {msg}")
            relatorio["alertas"].append({"tipo": "erro_leitura", "detalhe": msg})
            relatorio["status"] = "erro"
            continue

        registros = len(df)
        tamanho_kb = round(path.stat().st_size / 1024, 1)

        # Verifica volume mínimo
        minimo = ALERTA_VOLUME_MINIMO.get(nome_camada, {}).get(nome, 0)
        if registros < minimo:
            msg = f"{nome}: {registros} registros abaixo do mínimo esperado ({minimo})"
            log.warning(f"  ⚠ {msg}")
            relatorio["alertas"].append({"tipo": "volume_baixo", "detalhe": msg})
            if relatorio["status"] == "ok":
                relatorio["status"] = "alerta"

        # Verifica tamanho mínimo
        if tamanho_kb < ALERTA_TAMANHO_MINIMO_KB:
            msg = f"{nome}: arquivo muito pequeno ({tamanho_kb} KB)"
            log.warning(f"  ⚠ {msg}")
            relatorio["alertas"].append({"tipo": "arquivo_pequeno", "detalhe": msg})
            if relatorio["status"] == "ok":
                relatorio["status"] = "alerta"

        log.info(
            f"  ✔ {nome:<30} {registros:>6} registros  "
            f"{tamanho_kb:>8.1f} KB  lido em {latencia_ms}ms"
        )

        relatorio["arquivos"].append({
            "nome": nome,
            "registros": registros,
            "tamanho_kb": tamanho_kb,
            "latencia_leitura_ms": latencia_ms,
        })
        relatorio["total_registros"] += registros
        relatorio["total_tamanho_kb"] += tamanho_kb

    relatorio["total_tamanho_kb"] = round(relatorio["total_tamanho_kb"], 1)
    return relatorio


def checar_freshness(diretorio: Path, arquivos: list, max_horas: int = 24) -> list:
    """
    Verifica se os arquivos foram gerados recentemente.
    Alerta se algum arquivo for mais antigo que max_horas.
    """
    alertas = []
    agora = time.time()

    for nome in arquivos:
        path = diretorio / f"{nome}.parquet"
        if not path.exists():
            continue

        idade_horas = (agora - path.stat().st_mtime) / 3600
        if idade_horas > max_horas:
            msg = (
                f"{nome}.parquet gerado há {idade_horas:.1f}h "
                f"(limite: {max_horas}h)"
            )
            alertas.append({"tipo": "arquivo_desatualizado", "detalhe": msg})
            log.warning(f"  ⚠ {msg}")

    return alertas


def checar_streaming(streaming_dir: Path) -> dict:
    """
    Verifica o estado da fila de streaming.
    Alerta se houver muitos arquivos pendentes.
    """
    log.info("\n── Monitorando streaming ──")

    raw_dir = streaming_dir / "streaming_raw"
    processed_dir = streaming_dir / "streaming_processed"

    pendentes = len(list(raw_dir.glob("*.json"))) if raw_dir.exists() else 0
    processados = len(list(processed_dir.glob("*.json"))) if processed_dir.exists() else 0

    alertas = []
    if pendentes > 100:
        msg = f"Fila de streaming com {pendentes} eventos pendentes — consumer pode estar parado"
        log.warning(f"  ⚠ {msg}")
        alertas.append({"tipo": "fila_streaming_alta", "detalhe": msg})
    else:
        log.info(f"  ✔ Streaming: {pendentes} pendentes, {processados} processados")

    return {
        "pendentes": pendentes,
        "processados": processados,
        "alertas": alertas,
        "status": "alerta" if alertas else "ok",
    }


# ── Execução principal ────────────────────────────────────────────────────────

def run():
    inicio_total = time.time()

    log.info("=" * 60)
    log.info("Pipeline Monitor — início da verificação")
    log.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    # Monitora cada camada
    relatorio_bronze = checar_camada("bronze", BRONZE_DIR, BRONZE_ESPERADOS)
    relatorio_silver = checar_camada("silver", SILVER_DIR, SILVER_ESPERADOS)
    relatorio_gold   = checar_camada("gold",   GOLD_DIR,   GOLD_ESPERADOS)

    # Verifica freshness (arquivos atualizados nas últimas 24h)
    log.info("\n── Verificando freshness dos arquivos ──")
    alertas_freshness = []
    alertas_freshness += checar_freshness(BRONZE_DIR, BRONZE_ESPERADOS)
    alertas_freshness += checar_freshness(SILVER_DIR, SILVER_ESPERADOS)
    alertas_freshness += checar_freshness(GOLD_DIR, GOLD_ESPERADOS)
    if not alertas_freshness:
        log.info("  ✔ Todos os arquivos atualizados nas últimas 24h")

    # Monitora streaming
    relatorio_streaming = checar_streaming(ROOT / "data")

    # Tempo total
    duracao_s = round(time.time() - inicio_total, 2)

    # ── Relatório final ───────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("RELATÓRIO DE SAÚDE DO PIPELINE")
    log.info("=" * 60)

    camadas = [relatorio_bronze, relatorio_silver, relatorio_gold]
    todos_alertas = alertas_freshness.copy()

    for r in camadas:
        icone = "✔" if r["status"] == "ok" else ("⚠" if r["status"] == "alerta" else "✘")
        log.info(
            f"  {icone} {r['camada'].upper():<10} "
            f"{r['total_registros']:>7} registros  "
            f"{r['total_tamanho_kb']:>8.1f} KB  "
            f"status: {r['status']}"
        )
        todos_alertas += r["alertas"]

    # Streaming
    icone = "✔" if relatorio_streaming["status"] == "ok" else "⚠"
    log.info(
        f"  {icone} STREAMING   "
        f"pendentes: {relatorio_streaming['pendentes']}  "
        f"processados: {relatorio_streaming['processados']}"
    )
    todos_alertas += relatorio_streaming["alertas"]

    log.info("-" * 60)
    log.info(f"  Duração total da verificação: {duracao_s}s")
    log.info(f"  Total de alertas: {len(todos_alertas)}")

    if todos_alertas:
        log.info("\n  ALERTAS DETECTADOS:")
        for alerta in todos_alertas:
            log.warning(f"    ⚠ [{alerta['tipo']}] {alerta['detalhe']}")
    else:
        log.info("\n  ✔ Pipeline saudável — nenhum alerta detectado")

    log.info("=" * 60)

    status_geral = "ok" if not todos_alertas else "alerta"
    return {
        "status": status_geral,
        "duracao_s": duracao_s,
        "camadas": camadas,
        "streaming": relatorio_streaming,
        "alertas": todos_alertas,
    }


if __name__ == "__main__":
    run()
