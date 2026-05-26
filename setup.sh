#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# Setup do ambiente local — Tech Challenge Fase 2
# Execute: bash setup.sh
# ─────────────────────────────────────────────────────────────────

set -e  # para se qualquer comando falhar

echo "🔧 Verificando python3-venv..."
if ! python3 -m venv --help &>/dev/null; then
    echo "Instalando python3.12-venv (requer sudo)..."
    sudo apt install python3.12-venv -y
fi

echo "📦 Criando ambiente virtual (.venv)..."
python3 -m venv .venv

echo "⚡ Ativando ambiente e instalando dependências..."
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt

echo ""
echo "✅ Setup concluído!"
echo ""
echo "Para ativar o ambiente:"
echo "  source .venv/bin/activate"
echo ""
echo "Para rodar o pipeline Bronze:"
echo "  python pipeline/batch/bronze/bronze_loader.py"
echo ""
echo "Para abrir os notebooks:"
echo "  jupyter notebook"
