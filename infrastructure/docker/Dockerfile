# Coloquei essa imagem de exemplo, mas podemos seguir a do projeto sem problemas depois
FROM python:3.13-slim

# Definimos a pasta de trabalho dentro do contêiner
WORKDIR /app

# Copiamos o requirements.txt da raiz do projeto para dentro do contêiner
COPY requirements.txt .

# Instalamos as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos todo o resto do projeto para dentro do contêiner
COPY . .

# Variável de ambiente para o Python imprimir os logs na tela em tempo real
ENV PYTHONUNBUFFERED=1