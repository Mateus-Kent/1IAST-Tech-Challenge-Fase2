# 📚 Pipeline Híbrido para Análise da Alfabetização no Brasil

> **Tech Challenge – Fase 2 | PosTech FIAP | Engenharia de Dados**

---

## 👥 Equipe

Projeto desenvolvido para o **Tech Challenge – Fase 2 da PosTech FIAP**

* **Gabriela de Lima Lopes** (RM372467) ➔ [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/gabrieladelimalopes/)
* **Vitor Lopes Rodrigues** (RM372427) ➔ [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/vitor-lopes-rodrigues/)
* **Lucas Oliveira dos Santos Lima** (RM372651) ➔ [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/lucasoslima/)
* **Mateus Quintino Vieira dos Santos** (RM371795) ➔ [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mateusqsantos/)

---

## 📌 Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do país. O **Compromisso Nacional Criança Alfabetizada** é uma política pública que mobiliza União, estados e municípios com o objetivo de garantir que **todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental**.

O INEP definiu o **ponto de corte de 743 pontos** na escala de proficiência do SAEB como referência para considerar uma criança alfabetizada. Com base nisso, criou-se o **Indicador Criança Alfabetizada** — o percentual de estudantes que atingem esse nível. A **meta nacional é 80% até 2030**.

Este projeto constrói uma **pipeline híbrida de dados (Batch + Streaming)** para integrar, tratar e disponibilizar esses dados educacionais para análises, dashboards e modelos de machine learning.

---

## 🏗️ Arquitetura da Solução

### Visão Geral

```
┌──────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                               │
│         Base dos Dados (INEP) — Indicador Criança Alfabetizada       │
└────────────┬─────────────────────────────────────────────────────────┘
             │
     ┌───────┴────────┐
     │  INGESTÃO      │
     │  HÍBRIDA       │
     ├────────────────┤
     │  Batch         │  → Dados históricos (CSVs)
     │  Streaming     │  → Eventos simulados (JSON)
     └───────┬────────┘
             │
┌────────────▼──────────────────────────────────────────────────────┐
│                    ARQUITETURA MEDALHÃO (AWS S3)                   │
│                                                                    │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐  │
│  │   🥉 BRONZE  │ → │   🥈 SILVER  │ → │      🥇 GOLD         │  │
│  │  Dados Brutos│   │ Dados Tratados│   │  Camada Analítica    │  │
│  │  (Raw/Parquet)│  │ (Limpos/Join)│   │ (Pronto p/ consumo)  │  │
│  └──────────────┘   └──────────────┘   └──────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
             │
     ┌───────▼────────────────────────────────┐
     │          CONSUMO DE DADOS              │
     │  Dashboards │ ML Models │ Relatórios  │
     └────────────────────────────────────────┘
```

### Stack Tecnológica

| Camada | Tecnologia | Justificativa |
|---|---|---|
| **Processamento** | Python + Pandas | Flexível, suficiente para o volume atual |
| **Streaming** | Simulação via arquivos JSON | Simula eventos de atualização de indicadores |
| **Containerização** | Docker + Docker Compose | Ambiente local idêntico à produção |
| **Qualidade de Dados** | Python (quality_checker.py) | Validação customizada de duplicatas, nulos e relacionamentos |

---

## 📁 Estrutura do Repositório

```
1IAST-Tech-Challenge-Fase2/
│
├── 📂 data/
│   ├── raw/                          # CSVs originais do Base dos Dados
│   │   ├── meta_alfabetizacao_brasil.csv
│   │   ├── meta_alfabetizacao_municipio.csv
│   │   ├── meta_alfabetizacao_uf.csv
│   │   ├── indicador_municipio.csv
│   │   └── indicador_uf.csv
│   ├── streaming_raw/                # Eventos JSON gerados pelo producer
│   └── streaming_processed/         # Eventos processados pelo consumer
│
├── 📂 pipeline/
│   ├── batch/
│   │   ├── bronze/
│   │   │   └── bronze_loader.py      # Lê CSVs → salva Parquet na Bronze
│   │   ├── silver/
│   │   │   └── silver_loader.py      # Limpeza, tipagem e join das bases
│   │   └── gold/
│   │       └── gold_builder.py       # Gera datasets analíticos
│   ├── streaming/
│   │   ├── producer/
│   │   │   └── producer.py           # Simula eventos de atualização
│   │   └── consumer/
│   │       └── consumer.py           # Processa eventos JSON
│   └── dags/
│       └── pipeline_alfabetizacao.py # DAG Airflow — orquestra Bronze→Silver→Gold→Quality
│
├── 📂 layers/                        # Camadas locais geradas pelo pipeline
│   ├── bronze/                       # Parquet bruto
│   ├── silver/                       # Parquet limpo e integrado
│   └── gold/                         # Parquet analítico
│
├── 📂 validation/
│   └── quality_checks/
│       └── quality_checker.py        # Valida duplicatas, nulos, consistência e relacionamentos
│
├── 📂 infrastructure/
│   │
│   └── docker/
│       ├── Dockerfile
│       └── docker-compose.yml
│
├── 📂 notebooks/
│   ├── 01_eda.ipynb                  # Análise exploratória dos dados brutos
│   ├── 02_bronze_demo.ipynb          # Demonstração da camada Bronze
│   └── 03_gold_analise.ipynb         # Visualizações dos datasets Gold
│
├── 📂 docs/
│   └── diagrams/
│
├── 📂 tests/
│
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 🔄 Fluxo de Dados

### 1. Ingestão Batch (Dados Históricos)

```
CSVs (data/raw/)
      │
      ▼
bronze_loader.py
  - Lê os 5 CSVs do INEP
  - Adiciona metadados: _ingestion_timestamp, _source_file, _pipeline_version
  - Preserva dados brutos sem transformações
      │
      ▼
Bronze Layer (layers/bronze/ ou S3: s3://tech-challenge-fase2/bronze/)
  - Formato: Parquet
  - 34.901 registros processados
```

### 2. Ingestão Streaming (Eventos Simulados)

```
producer.py
  - Lê IDs reais de municípios do CSV
  - Gera eventos fictícios a cada 2-5 segundos
  - Tipos: nova_medicao_desempenho | atualizacao_meta
  - Salva cada evento como arquivo JSON em data/streaming_raw/
      │
      ▼
consumer.py
  - Monitora data/streaming_raw/ a cada 3 segundos
  - Processa cada evento JSON
  - Move arquivos processados para data/streaming_processed/
```

### 3. Processamento Silver

```
silver_loader.py (Airflow DAG: daily)
  - Lê da Bronze Layer
  - Remove colunas de metadados de ingestão
  - Converte tipos: cast para float64 e Int64
  - Validação Fail Fast: duplicatas e chaves nulas abortam o pipeline
  - JOIN: indicador_uf + meta_uf → uf_consolidado
  - JOIN: indicador_municipio + meta_municipio → municipio_consolidado
      │
      ▼
Silver Layer (layers/silver/)
  - 7 arquivos Parquet gerados
```

### 4. Camada Gold (Analítica)

```
gold_builder.py (Airflow DAG: após Silver)
  - ranking_uf:           Ranking dos 27 estados por taxa de alfabetização (2024)
  - meta_vs_realizado_uf: Comparação meta vs realizado com classificação de desempenho
  - evolucao_uf:          Variação 2023 → 2024 por estado (quem melhorou/piorou)
  - painel_nacional:      Visão consolidada nacional por ano
      │
      ▼
Gold Layer (layers/gold/)
  - 4 datasets prontos para dashboards, análises e modelos de ML
```

### 5. Orquestração (Airflow DAG)

```
bronze_ingestion → silver_transform → gold_build → quality_check
```

O DAG executa diariamente com retry automático (2 tentativas, intervalo de 5 min).
Se o quality_check detectar erros críticos, o pipeline é abortado e alertado.

---

## ✅ Qualidade de Dados

O `quality_checker.py` realiza 4 tipos de verificação nos dados da camada Bronze:

| Verificação | Descrição | Comportamento |
|---|---|---|
| **Duplicatas** | Detecta registros repetidos | Alerta se encontrar duplicatas |
| **Valores nulos** | Verifica % de nulos por coluna | Alerta se coluna > 20% nulos |
| **Consistência** | Verifica negativos e percentuais fora de [0,100] | Alerta por coluna |
| **Relacionamentos** | Valida integridade referencial entre tabelas | Alerta se encontrar órfãos |

**Resultado da última execução:**
- ✔ 14 verificações OK
- ⚠ 3 alertas (nulos esperados em 2023 — dados de nível só existem em 2024)
- ✘ 0 erros críticos

---

## 💡 Insights dos Dados

Alguns achados relevantes identificados na camada Gold:

- **CE** tem a maior taxa de alfabetização (85%) e já superou a meta de 2030
- **SE** e **BA** têm as menores taxas (~35-38%), muito abaixo da meta
- **RS** teve queda significativa em 2024 comparado a 2023
- **RR** (Roraima) não possui dados — ausência total de registros
- Dados de distribuição por nível de proficiência só existem para 2024

---

## 💰 FinOps – Otimização de Custos

| Prática | Impacto |
|---|---|
| **Parquet vs CSV** | Reduz armazenamento S3 em ~75% |
| **Particionamento** | Athena escaneia apenas partições relevantes |
| **S3 Intelligent-Tiering** | Move dados frios automaticamente para camadas mais baratas |
| **Athena serverless** | Paga apenas por query executada, sem cluster 24/7 |
| **Airflow no Docker** | Sem infraestrutura ociosa em ambiente local |

### Estimativa de Custo Mensal (AWS)

| Serviço | Estimativa |
|---|---|
| S3 (50 GB) | ~$1,15 |
| Athena (100 GB scanned/mês) | ~$5,00 |
| CloudWatch Logs + Métricas | ~$5,00 |
| **Total estimado** | **~$11/mês** |

---

## 📋 Decisões Arquiteturais

| Trade-off | Decisão | Justificativa |
|---|---|---|
| **Batch vs Streaming** | Híbrido | Dados históricos = batch; atualizações = streaming simulado |
| **Data Lake vs DWH** | Data Lake (S3) + Parquet | Menor custo, maior flexibilidade para ML |
| **Kafka vs JSON local** | JSON local (simulação) | Simplicidade para o escopo do projeto; Kafka seria o próximo passo |
| **Great Expectations vs Python** | Python customizado | Sem dependência externa, mais controle sobre as validações |
| **Airflow vs Step Functions** | Airflow | Familiar ao time, open-source, portável |
| **AWS vs GCP vs Azure** | AWS | Maior maturidade em dados, S3 como data lake |

---

## 🔭 Monitoramento

- **Falhas de ingestão:** Alerta se DAG falhar 2x seguidas
- **Latência:** Métrica por camada do pipeline
- **Volume:** Alerta se Bronze receber 0 registros em execução programada
- **Qualidade:** Quality checker falha o DAG se regras críticas não passarem

---

## 🤖 Aplicação em IA (Camada Gold)

A camada Gold está preparada para alimentar modelos de machine learning:

| Caso de Uso | Descrição |
|---|---|
| **Predição de alfabetização** | Usar taxa histórica + variáveis socioeconômicas para prever desempenho futuro por município |
| **Clustering de vulnerabilidade** | Agrupar municípios por perfil educacional para priorização de políticas públicas |
| **Análise de desigualdade** | Comparar gap entre regiões, redes pública/privada e evolução temporal |
| **Simulação de metas** | Projetar probabilidade de atingir meta 2030 dado o ritmo atual de cada estado |

---

## 🚀 Como Executar (Local)

```bash
# 1. Clone o repositório
git clone https://github.com/Mateus-Kent/1IAST-Tech-Challenge-Fase2.git
cd 1IAST-Tech-Challenge-Fase2

# 2. Instale dependências Python
pip install -r requirements.txt

# 3. Execute o pipeline batch em sequência
python pipeline/batch/bronze/bronze_loader.py
python pipeline/batch/silver/silver_loader.py
python pipeline/batch/gold/gold_builder.py

# 4. Valide qualidade dos dados
python validation/quality_checks/quality_checker.py

# 5. (Opcional) Inicie o streaming simulado em dois terminais separados
python pipeline/streaming/producer/producer.py
python pipeline/streaming/consumer/consumer.py
```

### Executar com Docker (só está com producer e consumer)

```bash
docker-compose -f infrastructure/docker/docker-compose.yml up -d
```



---

*Dados fornecidos por [Base dos Dados](https://basedosdados.org/) — Indicador Criança Alfabetizada (INEP/MEC)*
