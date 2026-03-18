<div align="center">

# Shadow Tax Ledger

### Pipeline de dados orientado a eventos para espelhamento tributário da Reforma Tributária brasileira

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1.8-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![AWS](https://img.shields.io/badge/AWS-LocalStack-FF9900?style=flat&logo=amazonaws&logoColor=white)](https://localstack.cloud)
[![Amazon S3](https://img.shields.io/badge/Amazon%20S3-Parquet-569A31?style=flat&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3)
[![Amazon SQS](https://img.shields.io/badge/Amazon%20SQS-Event--driven-FF4F8B?style=flat&logo=amazonsqs&logoColor=white)](https://aws.amazon.com/sqs)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Plotly](https://img.shields.io/badge/Plotly-Charts-3F4F75?style=flat&logo=plotly&logoColor=white)](https://plotly.com)
[![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?style=flat&logo=pydantic&logoColor=white)](https://docs.pydantic.dev)
[![Docker](https://img.shields.io/badge/Docker-LocalStack-2496ED?style=flat&logo=docker&logoColor=white)](https://docker.com)
[![LC](https://img.shields.io/badge/LC%20214%2F2025-IBS%2FCBS-009739?style=flat)](https://www.planalto.gov.br)

> *"Não basta conhecer a lei que virá — é preciso quantificar o seu impacto antes que ela chegue."*

</div>

---

## O que é um Shadow Tax Ledger

Um **Shadow Tax Ledger** — ou *Razão Tributário Sombra* — é um conceito originado na contabilidade forense e no compliance regulatório internacional. A ideia central é executar em paralelo um segundo sistema de cálculo tributário que espelha todas as operações do sistema principal, mas sob um conjunto diferente de regras.

O termo *"sombra"* não carrega conotação negativa — pelo contrário. É uma referência direta à técnica de *shadow accounting*, amplamente utilizada por instituições financeiras para simular o impacto de mudanças regulatórias antes da sua vigência. Bancos utilizam shadow ledgers para calcular provisões sob novas normas do Banco Central sem alterar o sistema contábil oficial. Seguradoras utilizam para simular Solvência II antes da implementação. O princípio é o mesmo: **operar o sistema atual e o sistema futuro em paralelo, comparando os resultados em tempo real**.

No contexto da Reforma Tributária brasileira, o Shadow Tax Ledger resolve um problema concreto: a transição do modelo fragmentado (ICMS/PIS/COFINS/IPI) para o IVA dual (IBS/CBS) não acontece do dia para a noite — há um período de coexistência entre 2026 e 2033. Durante esse período, empresas precisam calcular os dois regimes simultaneamente para fins de planejamento, precificação e compliance.

Este projeto automatiza exatamente esse processo: cada NF-e emitida no modelo atual gera automaticamente sua *nota sombra* calculada sob as regras da LC 214/2025, expondo o delta tributário de forma auditável e em tempo real.

---

## Por que este projeto existe

A promulgação da **Lei Complementar 214/2025** representa a maior reforma do sistema tributário brasileiro em mais de três décadas. A substituição do modelo fragmentado por um **IVA dual** composto pelo IBS e pela CBS, acrescido do Imposto Seletivo (IS), impõe às empresas um desafio imediato: **quanto vou pagar de imposto no novo regime?**

A resposta não é trivial. A carga tributária atual é calculada de forma cumulativa, com créditos limitados e alíquotas que variam por estado, produto e regime. O novo modelo opera com **crédito pleno e não-cumulatividade total** — o que, dependendo da cadeia produtiva e do NCM do produto, pode resultar em redução ou aumento significativo da carga.

O Shadow Tax Ledger foi construído para responder essa pergunta em tempo real, de forma automatizada e auditável.

---

## Arquitetura do pipeline

```
┌─────────────────────────────────────────────────────────────┐
│                    Orquestração                             │
│               Apache Airflow 3.1.8                          │
│          DAG: shadow_tax_pipeline (*/1 * * * *)             │
│     gerar_nfe → calcular_sombra → salvar_s3                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                    Ingestão                                 │
│  Faker NF-e (Focus NF-e schema · Pydantic v2)               │
│  Conformidade Fácil (tabela NCM → IBS/CBS · LC 214/2025)    │
│  API Senado (monitor PLPs 68, 108, 130)                     │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│                  Amazon SQS                                 │
│      nfe-raw · reforma-rules · plp-status                   │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│              Motor Tributário — Shadow Calculator           │
│  Carga atual:  ICMS + PIS + COFINS + IPI                    │
│  Carga futura: IBS + CBS + IS (por NCM · crédito pleno)     │
│  Delta: absoluto (R$) + percentual (%) + favorável (bool)   │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│           Amazon S3 + AWS Glue Data Catalog                 │
│   s3://shadow-tax-raw    → NF-es originais                  │
│   s3://shadow-tax-shadow → Notas Sombra (IBS/CBS)           │
│   s3://shadow-tax-delta  → Delta report por CNPJ/NCM        │
│   Formato: Parquet · particionado por ano/mes/dia           │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────┐
│              Streamlit Dashboard                            │
│   KPIs · gráficos comparativos · tabela detalhada           │
│   Status legislativo PLPs · atualização automática 30s      │
└─────────────────────────────────────────────────────────────┘
```

---

## Orquestração com Apache Airflow

O pipeline é orquestrado pelo **Apache Airflow 3.1.8** com schedule `*/1 * * * *` — uma NF-e processada por minuto, de forma totalmente automatizada. Cada execução do pipeline gera um DAG run rastreável, com logs completos de cada task, timestamps e status de sucesso ou falha com retry automático.

![Airflow DAG shadow_tax_pipeline](print_airflow.png)

A DAG `shadow_tax_pipeline` possui três tasks em sequência:

| Task | Descrição |
|---|---|
| `gerar_nfe` | Gera 1 NF-e por execução com Faker pt_BR no schema Focus NF-e |
| `calcular_sombra` | Lê a fila SQS e calcula IBS/CBS/IS por NCM, publicando a nota sombra |
| `salvar_s3` | Persiste as notas sombra e o delta report em Parquet particionado no S3 |

A escolha do Airflow — em detrimento de um simples cron ou loop Python — foi deliberada: em um contexto de compliance fiscal, **rastreabilidade de execução não é opcional**. O Airflow garante que cada NF-e processada tenha um registro auditável com horário, duração e resultado.

---

## Dashboard — visão em tempo real

O dashboard foi desenvolvido em **Streamlit** com design minimalista inspirado na estética gráfica dos anos 1960 — paleta Pantone, tipografia DM Sans + DM Mono, fundo creme `#F5F0E8` e blocos de KPI em cores sólidas. A escolha estética não é arbitrária: dashboards de compliance tributário são lidos por juristas e CFOs — a clareza visual é um requisito funcional.

### KPIs e visão executiva

![Dashboard Shadow Tax Ledger — KPIs e gráficos](dashboard2.png)

O painel superior apresenta cinco métricas-chave em tempo real:

- **NF-es analisadas** — volume do lote atual
- **Carga atual** — soma ICMS + PIS + COFINS + IPI em R$
- **Carga futura** — soma IBS + CBS + IS em R$
- **Delta total** — impacto financeiro absoluto do lote
- **Com redução** — percentual de NF-es com carga futura menor

Os dois gráficos comparativos mostram, por NF-e: (1) a comparação direta entre carga atual e futura em barras agrupadas; (2) o delta percentual com código de cor — verde para redução, laranja para aumento.

### Tabela detalhada e status legislativo

![Dashboard Shadow Tax Ledger — tabela e PLPs](dashboard1.png)

A tabela detalhada expõe, para cada NF-e: CNPJ do emitente, carga atual, carga futura, delta absoluto, delta percentual e indicador de redução. O painel de status legislativo monitora em tempo real a tramitação dos PLPs complementares — indicando se a simulação é baseada em lei já aprovada ou ainda em votação.

O dashboard atualiza automaticamente a cada **30 segundos**, consumindo os arquivos Parquet mais recentes do S3 via cache TTL.

---

## Insights e conclusões

Com base nas **70 NF-es processadas**:

| Métrica | Valor |
|---|---|
| Carga tributária atual (ICMS/PIS/COFINS/IPI) | **R$ 14,18M** |
| Carga tributária futura (IBS/CBS/IS) | **R$ 16,16M** |
| Delta total do lote | **+R$ 1,979K (+14%)** |
| NF-es com redução de carga | **41% (29 de 70)** |
| NF-es com aumento de carga | **59% (41 de 70)** |

**Casos extremos identificados:**
- NF-e 583844: delta de **+50,4%** — produto sujeito ao Imposto Seletivo
- NF-e 362991: delta de **-43,9%** — produto com regime de redução (dispositivos médicos)
- NF-e 108840: delta de **-35,5%** — produto com benefício fiscal na nova tabela NCM

**Sugestões estratégicas:**

Empresas com alto volume em NCMs de regime reduzido (ex: 90211000 — dispositivos médicos) devem acelerar o planejamento de transição — o novo modelo é claramente mais favorável para esse segmento. Por outro lado, operações com NCMs sujeitos ao IS (automóveis, bebidas) devem revisar sua estrutura de precificação com urgência, dado o impacto de até +50% na carga tributária.

O período de transição (2026–2033) deve ser utilizado para renegociação de contratos de fornecimento, revisão de tabelas de preço e reestruturação da cadeia produtiva com base no novo modelo de crédito pleno.

---

## Motor tributário — lógica de cálculo

### Modelo atual (fragmentado)
```
Carga atual = (Base × aliq_ICMS) + (Base × aliq_PIS) + (Base × aliq_COFINS) + (Base × aliq_IPI)
```

### Modelo futuro (IVA dual · LC 214/2025)
```
Carga futura = Base × (aliq_IBS + aliq_CBS + aliq_IS)
```

| Tributo | Competência | Alíquota referência |
|---|---|---|
| IBS | Estados e municípios | 17,8% |
| CBS | União (federal) | 8,8% |
| IS | União — produtos seletivos | 0% a 25% por NCM |

### Delta
```
Delta absoluto   = Carga futura − Carga atual
Delta percentual = (Delta absoluto / Carga atual) × 100
Favorável        = Delta absoluto < 0
```

---

## Status legislativo monitorado

| PLP | Descrição | Status |
|---|---|---|
| PLP 68 | IBS/CBS/IS — regras gerais do IVA dual | ✅ Lei vigente — LC 214/2025 |
| PLP 108 | IBS — regulamentação estadual/municipal | 🟡 Em tramitação no Senado |
| PLP 130 | CBS — regulamentação federal | 🟡 Em tramitação na Câmara |

---

## Tecnologias utilizadas

| Tecnologia | Versão | Papel no projeto |
|---|---|---|
| Python | 3.12 | Linguagem principal |
| Apache Airflow | 3.1.8 | Orquestração do pipeline |
| Amazon SQS | — | Mensageria event-driven |
| Amazon S3 | — | Data lake (raw/shadow/delta) |
| AWS Glue Catalog | — | Catálogo de metadados |
| Amazon Athena | — | Query SQL serverless |
| LocalStack | 4.x | Emulação AWS local |
| Docker | — | Container LocalStack |
| Faker (pt_BR) | 24.x | Geração de NF-es sintéticas |
| Pydantic | v2 | Validação de schema NF-e |
| Pandas | 2.x | Manipulação de dados |
| PyArrow | 15.x | Escrita Parquet |
| Streamlit | 1.x | Dashboard web |
| Plotly | 5.x | Gráficos interativos |
| boto3 | 1.x | SDK AWS Python |
| awscli-local | — | CLI LocalStack |

---

## Como rodar localmente

```bash
# Pré-requisitos: Docker + Python 3.11+

# 1. Clonar o repositório
git clone https://github.com/joaohppenha/shadow-tax-ledger.git
cd shadow-tax-ledger

# 2. Instalar dependências
pip install localstack awscli-local faker boto3 pydantic \
            pandas pyarrow streamlit plotly apache-airflow

# 3. Subir LocalStack
localstack start -d

# 4. Criar infraestrutura AWS local
awslocal sqs create-queue --queue-name nfe-raw
awslocal sqs create-queue --queue-name reforma-rules
awslocal sqs create-queue --queue-name plp-status
awslocal s3 mb s3://shadow-tax-raw
awslocal s3 mb s3://shadow-tax-shadow
awslocal s3 mb s3://shadow-tax-delta

# 5. Subir Airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
# Acesse http://localhost:8080 · user: admin
# Ative a DAG shadow_tax_pipeline

# 6. Subir o dashboard
streamlit run src/dashboard/app.py
# Acesse http://localhost:8501
```

---

## Estrutura do projeto

```
shadow-tax-ledger/
├── src/
│   ├── generator/
│   │   ├── nfe_generator.py        # Faker NF-e · Focus schema · Pydantic v2
│   │   ├── conformidade_facil.py   # Tabela NCM · LC 214/2025 · flag USE_REAL_API
│   │   ├── plp_monitor.py          # Monitor PLPs · API Senado dadosabertos
│   │   └── data/
│   │       └── ncm_reforma.json    # Alíquotas IBS/CBS/IS por NCM
│   ├── processor/
│   │   └── shadow_calculator.py    # Motor tributário · delta absoluto e percentual
│   ├── storage/
│   │   └── s3_writer.py            # Parquet particionado · S3 shadow + delta
│   └── dashboard/
│       └── app.py                  # Streamlit · design anos 60 Pantone · Plotly
├── infrastructure/
│   └── setup_localstack.sh
└── dags/
    └── shadow_tax_pipeline.py      # Airflow DAG · */1 * * * * · 3 tasks
```

---

## Roadmap

- [x] Gerador NF-e com Faker (Focus NF-e schema · Pydantic v2)
- [x] Motor IBS/CBS/IS por NCM (LC 214/2025)
- [x] Pipeline event-driven SQS → S3 Parquet particionado
- [x] Orquestração Apache Airflow 3.1.8 com DAG visual
- [x] Monitor PLPs via API Senado (dadosabertos.senado.leg.br)
- [x] Tabela NCM Conformidade Fácil com flag para API real
- [x] Dashboard Streamlit com atualização automática a cada 30s
- [ ] Integração API RTC — validação CBS Receita Federal
- [ ] Deploy AWS com Terraform (IaC)
- [ ] Testes unitários do motor tributário (pytest)
- [ ] Alertas CloudWatch para variações de delta acima de 30%
- [ ] Relatório PDF exportável por CNPJ

---

## Referências

- [Lei Complementar 214/2025 — Planalto](https://www.planalto.gov.br)
- [PLP 68/2024 — Câmara dos Deputados](https://www.camara.leg.br)
- [API Dados Abertos Senado](https://dadosabertos.senado.leg.br)
- [Conformidade Fácil — SVRS](https://cff.svrs.rs.gov.br)
- [Focus NF-e Schema — NF-e.io](https://nfe.io/docs)
- [Calculadora RTC — Receita Federal](https://www.gov.br/receitafederal)

---

<div align="center">

**João Henrique Penha**


[![GitHub](https://img.shields.io/badge/GitHub-joaohppenha-181717?style=flat&logo=github)](https://github.com/joaohppenha)

*Este projeto é uma simulação técnica para fins de portfólio e estudo. As alíquotas utilizadas são baseadas nas referências publicadas na LC 214/2025 e podem sofrer alterações conforme regulamentação dos PLPs complementares.*

</div>
