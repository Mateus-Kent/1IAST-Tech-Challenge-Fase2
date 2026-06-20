import json
import time
import shutil
import logging
from pathlib import Path

# Configuração de Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Configuração de Diretórios
ROOT = Path(__file__).resolve().parents[3] 
RAW_DIR = ROOT / "data" / "streaming_raw"
PROCESSED_DIR = ROOT / "data" / "streaming_processed"

def setup_directories():
    """Garante que as pastas existam antes de começar a procurar arquivos."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def process_event(filepath: Path):
    """Lê o evento JSON, simula o processamento e move o arquivo."""
    try:
        # 1. Lê o arquivo JSON
        with open(filepath, 'r', encoding='utf-8') as f:
            event = json.load(f)
        
        # 2. Extrai as informações de negócio
        event_id = event.get('event_id')
        event_type = event.get('event_type')
        id_mun = event.get('id_municipio')
        taxa_nova = event.get('taxa_alfabetizacao_atualizada')
        
        # 3. Simula a atualização (aqui, na vida real, faríamos um UPDATE no banco/Silver)
        log.info(f"⚙️ Processando {event_id} | Município {id_mun} atualizou a taxa para {taxa_nova}% ({event_type})")
        
        # 4. Faxina: Move o arquivo para a pasta de processados
        dest_path = PROCESSED_DIR / filepath.name
        shutil.move(str(filepath), str(dest_path))
        
    except json.JSONDecodeError:
        log.error(f"Erro: JSON corrompido em {filepath.name}. Ignorando.")
    except Exception as e:
        log.error(f"Erro inesperado ao processar {filepath.name}: {e}")

def run():
    log.info("=" * 60)
    log.info("Iniciando o CONSUMER (Processador de Streaming)")
    log.info("=" * 60)
    
    setup_directories()
    log.info(f"Radar ativado. Vigiando a pasta: {RAW_DIR}")
    log.info("Pressione Ctrl+C para desligar o consumidor.\n")

    try:
        while True:
            # Busca todos os arquivos JSON que estão na pasta no momento
            json_files = list(RAW_DIR.glob("*.json"))
            
            if json_files:
                log.info(f"📥 Lote encontrado: {len(json_files)} novos eventos na fila.")
                for file_path in json_files:
                    process_event(file_path)
            
            # Pausa de 3 segundos para não sobrecarregar a CPU antes de checar a pasta de novo
            time.sleep(3)
            
    except KeyboardInterrupt:
        log.info("\n" + "=" * 60)
        log.info("Consumer interrompido pelo usuário. Desligando radar.")
        log.info("=" * 60)

if __name__ == "__main__":
    run()