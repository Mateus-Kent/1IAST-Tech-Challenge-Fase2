import csv
import json
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timezone

# Configuração de Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Configuração de Diretórios
ROOT = Path(__file__).resolve().parents[3] 
RAW_DIR = ROOT / "data" / "raw"
STREAMING_DIR = ROOT / "data" / "streaming_raw"

def load_municipio_ids() -> list:
    """Lê o CSV original da camada raw para pegar IDs reais dos municípios."""
    csv_path = RAW_DIR / "indicador_municipio.csv"
    ids = set() # Usamos set para garantir IDs únicos/ nao quis fazer aleatorio pq acredito que atrapalha na camada silver
    
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("id_municipio"):
                    ids.add(row["id_municipio"])
        log.info(f"Carregados {len(ids)} IDs únicos de municípios.")
        return list(ids)
    except Exception as e:
        log.error(f"Erro ao ler CSV: {e}")
        # Retorna alguns IDs de fallback caso o arquivo não seja encontrado
        return ["1100015", "1100023", "1100031", "3550308"]

def generate_event(municipio_ids: list) -> dict:
    """Gera um evento fictício com dados realistas para um município sorteado."""
    event_types = ["nova_medicao_desempenho", "atualizacao_meta"]
    
    event = {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": random.choice(event_types),
        "id_municipio": random.choice(municipio_ids),
        # Simula uma nova taxa de alfabetização variando de 40% a 100%
        "taxa_alfabetizacao_atualizada": round(random.uniform(40.0, 100.0), 2)
    }
    return event

def run():
    log.info("=" * 60)
    log.info("Iniciando o PRODUCER (Simulador de Streaming)")
    log.info("=" * 60)
    
    STREAMING_DIR.mkdir(parents=True, exist_ok=True)
    municipio_ids = load_municipio_ids()
    
    log.info(f"Os eventos gerados serão salvos na pasta: {STREAMING_DIR}")
    log.info("Pressione Ctrl+C no terminal para parar a simulação.\n")

    try:
        while True:
            # 1. Gera o evento sorteado com dados realistas para um município
            event = generate_event(municipio_ids)
            
            # 2. Define o nome do arquivo JSON baseado no timestamp para não sobrescrever
            safe_timestamp = event['timestamp'].replace(':', '').replace('-', '')
            filename = f"{safe_timestamp}_{event['event_id']}.json"
            filepath = STREAMING_DIR / filename
            
            # 3. Salva o evento fisicamente como um arquivo JSON
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(event, f, ensure_ascii=False, indent=4)
                
            log.info(f"🚀 Evento disparado: {event['event_type']:<25} | Município: {event['id_municipio']} | Taxa: {event['taxa_alfabetizacao_atualizada']}%")
            
            # 4. Pausa o script de forma aleatória entre 2 e 5 segundos para simular a vida real, no caso quando se coloca os dados automaticamente, não tem como controlar o tempo de chegada dos dados, então é interessante colocar uma variação para simular isso
            time.sleep(random.randint(2, 5))
            
    except KeyboardInterrupt:
        log.info("\n" + "=" * 60)
        log.info("Producer interrompido pelo usuário. Fim da transmissão.")
        log.info("=" * 60)

if __name__ == "__main__":
    run()