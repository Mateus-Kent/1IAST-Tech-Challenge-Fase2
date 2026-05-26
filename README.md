# 📚 Pipeline Híbrido para Análise da Alfabetização no Brasil

> **Tech Challenge – Fase 2 | PosTech FIAP | Engenharia de Dados**

---

## 📌 Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do país. O **Compromisso Nacional Criança Alfabetizada** é uma política pública que mobiliza União, estados e municípios com o objetivo de garantir que **todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental**.

O INEP definiu o **ponto de corte de 743 pontos** na escala de proficiência do SAEB como referência para considerar uma criança alfabetizada. Com base nisso, criou-se o **Indicador Criança Alfabetizada** — o percentual de estudantes que atingem esse nível. A **meta nacional é 100% até 2030**.

Este projeto constrói uma **pipeline híbrida de dados (Batch + Streaming)** para integrar, tratar e disponibilizar esses dados educacionais para análises, dashboards e modelos de machine learning.

---

## 🏗️ Arquitetura da Solução

### Visão Geral

```
┌──────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                               │
│  Base dos Dados (INEP) │ Censo Escolar │ IBGE │ FUNDEB              │
└────────────┬─────────────────────────────────────────────────────────┘
             │
     ┌───────┴────────┐
     │  INGESTÃO      │
     │  HÍBRIDA       │
     ├────────────────┤
     │  Batch         │  → Dados históricos (CSVs, APIs)
     │  Streaming     │  → Eventos simulados (Kafka/Kinesis)
     └───────┬────────┘
             │
┌────────────▼──────────────────────────────────────────────────────┐
│                    ARQUITETURA MEDALHÃO (AWS S3)                   │
│                                                                    │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐  │
│  │   🥉 BRONZE  │ → │   🥈 SILVER  │ → │      🥇 GOLD         │  │
│  │  Dados Brutos│   │ Dados Tratados│   │  Camada Analítica    │  │
│  │  (Raw/Parquet)│   │ (Limpos/Join)│   │ (Pronto p/ consumo)  │  │
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
| **Orquestração Batch** | Apache Airflow | Agendamento, retry automático, observabilidade |
| **Processamento** | Python + PySpark | Flexível, escalável para grandes volumes |
| **Streaming** | Apache Kafka + Kafka Streams | Baixa latência, tolerância a falhas |
| **Armazenamento** | AWS S3 (Parquet + particionado) | Custo baixo, integração nativa com AWS |
| **Catálogo de Dados** | AWS Glue Data Catalog | Governança, schema discovery automático |
| **Query Engine** | AWS Athena | SQL serverless sobre S3, paga por query |
| **Qualidade de Dados** | Great Expectations | Validação declarativa, relatórios automáticos |
| **IaC** | Terraform | Infraestrutura como código, reprodutível |
| **Containerização** | Docker + Docker Compose | Ambiente local idêntico à produção |
| **Monitoramento** | CloudWatch + Grafana | Alertas, dashboards operacionais |

---

## 📁 Estrutura do Repositório

```
1IAST-Tech-Challenge-Fase2/
│
├── 📂 data/
│   ├── raw/                          # CSVs originais baixados do Base dos Dados
│   │   ├── meta_alfabetizacao_brasil.csv
│   │   ├── meta_alfabetizacao_municipio.csv
│   │   ├── meta_alfabetizacao_uf.csv
│   │   ├── indicador_municipio.csv
│   │   └── indicador_uf.csv
│   └── samples/                      # Amostras reduzidas para testes locais
│
├── 📂 pipeline/
│   ├── batch/
│   │   ├── ingestion/                # Scripts de ingestão batch das fontes
│   │   │   └── ingest_base_dados.py  # Lê CSVs → salva Bronze (Parquet)
│   │   ├── bronze/                   # Transformações Bronze → raw Parquet
│   │   │   └── bronze_loader.py
│   │   ├── silver/                   # Transformações Silver (limpeza + join)
│   │   │   └── silver_transformer.py
│   │   └── gold/                     # Transformações Gold (datasets analíticos)
│   │       └── gold_builder.py
│   └── streaming/
│       ├── producer/                 # Simulação de eventos em tempo real
│       │   └── event_producer.py     # Publica atualizações de indicadores
│       └── consumer/                 # Consome e processa eventos Kafka
│           └── event_consumer.py
│
├── 📂 layers/                        # Simulação local das camadas (dev/test)
│   ├── bronze/                       # Parquet particionado por ano/uf
│   ├── silver/                       # Parquet limpo e integrado
│   └── gold/                         # Parquet analítico pronto para consumo
│
├── 📂 validation/
│   └── quality_checks/
│       ├── expectations/             # Regras Great Expectations (YAML/JSON)
│       └── run_quality_checks.py     # Executa validações e gera relatório
│
├── 📂 monitoring/
│   └── alerts/
│       ├── cloudwatch_alarms.tf      # Alarmes AWS CloudWatch via Terraform
│       └── pipeline_metrics.py       # Coleta e publica métricas customizadas
│
├── 📂 infrastructure/
│   ├── terraform/
│   │   ├── main.tf                   # Recursos principais (S3, Glue, Athena)
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── modules/
│   │       ├── s3/                   # Buckets Bronze/Silver/Gold
│   │       ├── glue/                 # Crawlers e Jobs
│   │       ├── kinesis/              # Streams para ingestão streaming
│   │       └── msk/                  # Amazon MSK (Kafka gerenciado)
│   └── docker/
│       ├── Dockerfile
│       └── docker-compose.yml        # Airflow + Kafka + Zookeeper local
│
├── 📂 docs/
│   ├── architecture/
│   │   └── decisions.md              # ADRs – Architecture Decision Records
│   └── diagrams/
│       └── pipeline_diagram.png      # Diagrama visual da pipeline
│
├── 📂 tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
│
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md                         # Este arquivo
```

---

## 🔄 Fluxo de Dados

### 1. Ingestão Batch (Dados Históricos)

```
Base dos Dados (CSV/API)
        │
        ▼
ingest_base_dados.py
  - Lê CSVs locais ou via API BigQuery
  - Adiciona metadados: ingestion_timestamp, source_file, pipeline_version
        │
        ▼
Bronze Layer (S3: s3://alfabetizacao-datalake/bronze/)
  - Formato: Parquet
  - Particionamento: ano=XXXX/uf=XX/
  - Sem transformações — dados brutos preservados
```

### 2. Ingestão Streaming (Eventos Simulados)

```
event_producer.py
  - Simula atualizações de indicadores (novos resultados SAEB)
  - Publica mensagens JSON no tópico Kafka: indicadores-alfabetizacao
        │
        ▼
Apache Kafka (MSK / local Docker)
        │
        ▼
event_consumer.py
  - Consome eventos do tópico
  - Valida schema, aplica transformações
  - Escreve micro-batches na Bronze Layer
```

### 3. Processamento Silver

```
silver_transformer.py (Airflow DAG: daily)
  - Lê da Bronze Layer
  - Limpeza: remove duplicatas, trata nulos, padroniza tipos
  - JOIN das bases: indicador_municipio + meta_municipio + dados_uf
  - Normaliza chaves: id_municipio (7 dígitos), sigla_uf (2 chars uppercase)
  - Valida consistência: taxa_alfabetizacao ∈ [0, 100]
        │
        ▼
Silver Layer (S3: s3://alfabetizacao-datalake/silver/)
  - Formato: Parquet (compressão Snappy)
  - Particionamento: ano=XXXX/
```

### 4. Camada Gold (Analítica)

```
gold_builder.py (Airflow DAG: após Silver)
  - Cria datasets temáticos prontos para consumo:
    ├── gold_indicador_por_municipio    → ranking e evolução por município
    ├── gold_comparacao_meta_resultado  → % atingimento das metas 2024–2030
    └── gold_evolucao_temporal          → série histórica nacional e por UF
        │
        ▼
Gold Layer (S3: s3://alfabetizacao-datalake/gold/)
  - Formato: Parquet (otimizado para Athena)
  - Consumido por: dashboards, modelos ML, relatórios
```

---

## ✅ Qualidade de Dados

| Regra | Implementação |
|---|---|
| Sem duplicatas | `DROP DUPLICATES` por chave composta (ano + id_municipio + rede) |
| Valores ausentes | Imputa media da UF ou flag `dados_ausentes=True` |
| Chaves válidas | Valida `id_municipio` contra tabela IBGE de municípios |
| Consistência | Taxa ∈ [0,100]; ano ∈ [2019, ano_atual] |
| Integridade referencial | Todo município na tabela de metas existe na tabela de indicadores |

Ferramenta: **Great Expectations** — gera data docs HTML automáticos a cada execução.

---

## 💰 FinOps – Otimização de Custos

| Prática | Impacto |
|---|---|
| **Parquet + Snappy** | Reduz armazenamento S3 em ~75% vs CSV |
| **Particionamento por ano/uf** | Athena escaneia apenas partições relevantes (paga por TB lido) |
| **S3 Intelligent-Tiering** | Move dados frios automaticamente para camadas mais baratas |
| **Athena vs Redshift** | Serverless: paga apenas por query executada, sem cluster 24/7 |
| **Spot Instances (EMR)** | Reduz custo de processamento Spark em até 70% |
| **Airflow no Fargate** | Sem EC2 ocioso; escala a zero quando não há DAGs rodando |

### Estimativa de Custo Mensal (AWS)

| Serviço | Estimativa |
|---|---|
| S3 (50 GB) | ~$1,15 |
| Athena (100 GB scanned/mês) | ~$5,00 |
| MSK Serverless (Kafka) | ~$20,00 |
| MWAA (Airflow) / ECS Fargate | ~$30,00 |
| CloudWatch Logs + Métricas | ~$5,00 |
| **Total estimado** | **~$61/mês** |

---

## 🔭 Monitoramento

- **Falhas de ingestão:** Alerta CloudWatch se DAG falhar 2x seguidas
- **Latência:** Métrica customizada `pipeline_duration_seconds` por camada
- **Volume:** Alerta se Bronze receber 0 registros em execução programada
- **Qualidade:** Great Expectations falha o DAG se regras críticas não passarem

---

## 🤖 Aplicação em IA (Camada Gold)

A camada Gold está preparada para:

| Caso de Uso | Descrição |
|---|---|
| **Predição de alfabetização** | Features: IDH municipal, infraestrutura escolar, % docentes formados → target: taxa_alfabetizacao |
| **Clustering de vulnerabilidade** | Agrupa municípios por perfil educacional para priorização de políticas |
| **Análise de desigualdade** | Compara gap entre regiões, redes pública/privada, evolução temporal |
| **Simulação de metas** | Projeta probabilidade de atingir meta 2030 dado o ritmo atual |

---

## 🚀 Como Executar (Local)

```bash
# 1. Clone o repositório
git clone <repo-url>
cd 1IAST-Tech-Challenge-Fase2

# 2. Configure variáveis de ambiente
cp .env.example .env
# edite .env com suas credenciais AWS

# 3. Suba a infraestrutura local (Kafka + Airflow)
docker-compose -f infrastructure/docker/docker-compose.yml up -d

# 4. Instale dependências Python
pip install -r requirements.txt

# 5. Execute ingestão batch manual
python pipeline/batch/ingestion/ingest_base_dados.py

# 6. Execute as transformações em sequência
python pipeline/batch/bronze/bronze_loader.py
python pipeline/batch/silver/silver_transformer.py
python pipeline/batch/gold/gold_builder.py

# 7. Valide qualidade dos dados
python validation/quality_checks/run_quality_checks.py

# 8. Inicie o producer de streaming (simulação)
python pipeline/streaming/producer/event_producer.py
```

---

## 📋 Decisões Arquiteturais

| Trade-off | Decisão | Justificativa |
|---|---|---|
| **Batch vs Streaming** | Híbrido | Dados históricos = batch; atualizações SAEB = streaming |
| **Data Lake vs DWH** | Data Lake (S3) + Athena | Menor custo, maior flexibilidade para ML |
| **Parquet vs Delta Lake** | Parquet (v1) → Delta Lake (roadmap) | Simplicidade inicial; Delta para ACID no futuro |
| **Airflow vs Step Functions** | Airflow | Familiar ao time, open-source, portável |
| **AWS vs GCP** | AWS | Maior maturidade em dados, MSK para Kafka gerenciado |

---

## 👥 Equipe

Projeto desenvolvido para o **Tech Challenge – Fase 2 da PosTech FIAP**

---

*Dados fornecidos por [Base dos Dados](https://basedosdados.org/) — Indicador Criança Alfabetizada (INEP/MEC)*
