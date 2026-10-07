# BanVic Data Engineering POC

Proof of Concept de Engenharia de Dados para ingestão, orquestração e validação dos dados de um ERP bancário fictício (BanVic).

O projeto implementa uma pipeline ELT executada em Kubernetes/Kind, com Apache Airflow para orquestração, Meltano para ingestão dos arquivos CSV e PostgreSQL como camada de destino (`raw`).

## 1. Visão geral

O objetivo deste POC é demonstrar uma infraestrutura reprodutível e uma pipeline de dados simples, modular e observável, cobrindo:

- provisionamento de infraestrutura com Kubernetes/Kind e Terraform;
- execução do Apache Airflow em Kubernetes;
- ingestão independente de sete arquivos CSV usando Meltano;
- carga dos dados em PostgreSQL na camada `raw`;
- sensores para aguardar a disponibilidade dos arquivos;
- validação da quantidade de registros após a carga;
- retries e timeout nas etapas de ingestão;
- comportamento idempotente para uma carga full snapshot;
- armazenamento de credenciais em Kubernetes Secrets;
- persistência dos dados PostgreSQL e dos logs do Airflow;
- scripts de bootstrap e verificação do ambiente.

## 2. Arquitetura

![Arquitetura da Plataforma do POC](docs/banvic_arquitetura.png)

### Fluxo da plataforma

```text
                       ┌─────────────────────────┐
                       │ 7 arquivos CSV em       │
                       │ data/raw/               │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │ Volume Kubernetes       │
                       │ banvic-raw-data-pvc     │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │ Apache Airflow          │
                       │ Scheduler               │
                       │                         │
                       │ 7 FileSensors           │
                       │         ↓               │
                       │ 7 tasks de ingestão     │
                       │         ↓               │
                       │ 1 summary               │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │ Meltano                 │
                       │ tap-csv                 │
                       │         ↓               │
                       │ target-postgres         │
                       └────────────┬────────────┘
                                    │
                                    ▼
                       ┌─────────────────────────┐
                       │ PostgreSQL              │
                       │ database: banvic        │
                       │ schema: raw             │
                       └─────────────────────────┘
```

Os CSVs são disponibilizados ao Scheduler por meio de um PersistentVolume/PersistentVolumeClaim. Eles **não são copiados para dentro da imagem Docker do Airflow**.

A imagem customizada contém o código da DAG, o projeto Meltano, suas configurações e as dependências necessárias para a execução da pipeline.

## 3. Tecnologias

| Tecnologia | Uso |
|---|---|
| Docker | Construção da imagem customizada do Airflow |
| Kind | Cluster Kubernetes local |
| Kubernetes | Execução e gerenciamento dos componentes |
| Terraform | Provisionamento declarativo da infraestrutura |
| Helm | Instalação e atualização do Airflow |
| Apache Airflow 3.2.2 | Orquestração da pipeline |
| Meltano 4.2.2 | Extração e carregamento dos CSVs |
| tap-csv | Extractor dos arquivos CSV |
| target-postgres | Loader para PostgreSQL |
| PostgreSQL 16.15 | Armazenamento dos dados |
| Python | Implementação da DAG e validações |
| PowerShell | Automação do bootstrap e verificação no Windows |

### Versões principais

- Airflow: `3.2.2`
- Helm chart Apache Airflow: `1.22.0`
- Meltano: `4.2.2`
- PostgreSQL: `16.15-alpine3.24`
- Kind node image: `kindest/node:v1.35.8`
- Terraform: `>= 1.6.0`

## 4. Estrutura do projeto

```text
banvic-data-engineering/
│
├── dags/
│   └── banvic_elt.py
│
├── data/
│   └── raw/
│       ├── agencias.csv
│       ├── clientes.csv
│       ├── colaborador_agencia.csv
│       ├── colaboradores.csv
│       ├── contas.csv
│       ├── propostas_credito.csv
│       └── transacoes.csv
│
├── docs/
│   ├── banvic_arquitetura.png
│   └── banvic_modelo_conceitual.png
│
├── infra/
│   ├── airflow/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── values.yaml
│   │
│   ├── kind/
│   │   └── cluster.yaml
│   │
│   ├── postgres/
│   │   ├── deployment.yaml
│   │   ├── pvc.yaml
│   │   └── service.yaml
│   │
│   ├── terraform/
│   │   ├── .gitignore
│   │   ├── cluster/
│   │   │   ├── .terraform.lock.hcl
│   │   │   ├── main.tf
│   │   │   ├── outputs.tf
│   │   │   ├── providers.tf
│   │   │   ├── variables.tf
│   │   │   └── versions.tf
│   │   │
│   │   └── platform/
│   │       ├── .terraform.lock.hcl
│   │       ├── airflow.tf
│   │       ├── namespace.tf
│   │       ├── postgres-init.tf
│   │       ├── postgres.tf
│   │       ├── providers.tf
│   │       ├── random.tf
│   │       ├── secrets.tf
│   │       ├── storage.tf
│   │       ├── variables.tf
│   │       └── versions.tf
│   │
│   └── namespace.yaml
│
├── meltano/
│   ├── config/
│   │   └── csv_files_definition.json
│   ├── meltano.yml
│   └── plugins/
│       ├── extractors/
│       │   └── tap-csv--meltanolabs.lock
│       └── loaders/
│           └── target-postgres--meltanolabs.lock
│
├── scripts/
│   ├── bootstrap.ps1
│   └── verify.ps1
│
├── .dockerignore
├── .gitignore
├── 1_GITHUB.txt
├── 2_VIDEO.txt
└── README.md
```

## 5. Dados de entrada

O POC utiliza sete arquivos CSV fornecidos pelo desafio:

| Arquivo | Destino | Registros validados |
|---|---|---:|
| `agencias.csv` | `raw.agencias` | 10 |
| `clientes.csv` | `raw.clientes` | 998 |
| `colaborador_agencia.csv` | `raw.colaborador_agencia` | 100 |
| `colaboradores.csv` | `raw.colaboradores` | 100 |
| `contas.csv` | `raw.contas` | 999 |
| `propostas_credito.csv` | `raw.propostas_credito` | 2.000 |
| `transacoes.csv` | `raw.transacoes` | 71.999 |

Os arquivos ficam versionados em `data/raw/` e são disponibilizados ao Scheduler por um volume persistente.

Eles não são embutidos na imagem Docker.

## 6. Estratégia de ingestão

A ingestão foi implementada como um ELT simples:

```text
CSV
  ↓
FileSensor
  ↓
tap-csv
  ↓
target-postgres
  ↓
PostgreSQL / raw
  ↓
Validação da quantidade de registros
```

Cada arquivo é tratado de forma independente.

### Extract

O `tap-csv` lê os sete arquivos definidos em:

```text
meltano/config/csv_files_definition.json
```

A configuração define:

- caminho dos arquivos;
- delimitador `,`;
- encoding UTF-8;
- modo estrito (`strict: true`);
- chave de cada stream;
- inclusão de colunas de metadados.

### Load

O `target-postgres` grava os dados no PostgreSQL usando:

```yaml
default_target_schema: raw
load_method: overwrite
validate_records: true
activate_version: false
```

A estratégia `overwrite` foi escolhida porque o POC trabalha com uma fotografia completa dos arquivos de origem. Cada execução recompõe as tabel
