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

A estratégia `overwrite` foi escolhida porque o POC trabalha com uma fotografia completa dos arquivos de origem. Cada execução recompõe as tabelas de destino com o snapshot atual.

Isso torna a carga idempotente do ponto de vista do conteúdo: executar novamente o mesmo conjunto de arquivos produz novamente o mesmo estado esperado da camada `raw`.

## 7. Orquestração com Airflow

A DAG principal é:

```text
banvic_elt
```

Arquivo:

```text
dags/banvic_elt.py
```

### Configuração da DAG

```text
schedule = 0 6 * * *
catchup = False
max_active_runs = 1
max_active_tasks = 2
retries = 2
retry_delay = 1 minuto
```

A DAG possui **15 tasks**:

```text
7 FileSensors
7 tasks de ingestão
1 task de resumo
```

### Fluxo da DAG

```text
wait_for_clientes
        ↓
ingest_clientes
        ┐
        │
wait_for_contas
        ↓
ingest_contas
        ┤
        │
wait_for_transacoes
        ↓
ingest_transacoes
        ┤
        │
wait_for_propostas_credito
        ↓
ingest_propostas_credito
        ┤
        │
wait_for_colaboradores
        ↓
ingest_colaboradores
        ┤
        │
wait_for_agencias
        ↓
ingest_agencias
        ┤
        │
wait_for_colaborador_agencia
        ↓
ingest_colaborador_agencia
        ┘
        ↓
summarize_ingestion
```

As sete cadeias de ingestão são independentes entre si e ficam limitadas pela configuração `max_active_tasks = 2`.

### FileSensor

Cada arquivo possui um sensor independente configurado com:

```text
fs_conn_id = fs_default
filepath = nome_do_arquivo.csv
poke_interval = 30 segundos
timeout = 600 segundos
mode = reschedule
```

O sensor aguarda a disponibilidade do arquivo antes de iniciar a respectiva ingestão.

### Tasks de ingestão

Cada task:

1. verifica as variáveis necessárias da conexão com PostgreSQL;
2. conta os registros existentes no CSV;
3. executa somente o stream correspondente no Meltano;
4. utiliza `--select` para selecionar explicitamente aquele stream;
5. executa `--full-refresh`;
6. consulta o PostgreSQL;
7. compara a quantidade carregada com a quantidade existente na origem.

O comando executado segue o padrão:

```text
meltano el tap-csv target-postgres --select <stream> --full-refresh
```

A conexão utilizada para validação é obtida por:

```python
PostgresHook(postgres_conn_id="banvic_postgres")
```

### Task de resumo

A task `summarize_ingestion` recebe os resultados das sete ingestões e registra no log a quantidade validada de cada tabela.

## 8. Confiabilidade e tratamento de falhas

O POC inclui mecanismos básicos de confiabilidade.

### Retries

As tasks da DAG herdam:

```text
retries = 2
retry_delay = 1 minuto
```

Isso permite recuperar falhas transitórias sem intervenção manual imediata.

### Timeout

Cada task de ingestão possui:

```text
execution_timeout = 15 minutos
```

### Idempotência

O loader PostgreSQL está configurado com:

```text
load_method: overwrite
```

e cada ingestão usa:

```text
--full-refresh
```

Como os dados são tratados como snapshot completo, executar novamente a pipeline recompõe as tabelas `raw` a partir dos arquivos de origem.

### Controle de concorrência

A DAG utiliza:

```text
max_active_runs = 1
max_active_tasks = 2
```

Isso impede execuções concorrentes da mesma DAG e limita o número de tasks simultâneas.

Além disso, o Airflow está configurado com:

```text
AIRFLOW__CORE__PARALLELISM = 2
```

### Validação de origem

Cada task lê diretamente o respectivo CSV e calcula sua quantidade de registros antes de iniciar o Meltano.

### Validação de destino

Após a ingestão, a task consulta:

```text
raw.<tabela>
```

e compara a quantidade de registros carregados com a quantidade calculada na origem.

Uma divergência faz a task falhar.

## 9. PostgreSQL

O PostgreSQL roda no namespace:

```text
banvic
```

Serviço Kubernetes:

```text
banvic-postgres
```

Porta:

```text
5432
```

O PostgreSQL hospeda:

- database `banvic`, utilizado pela pipeline;
- database `airflow`, utilizado pelos metadados do Airflow.

O banco `banvic` possui o schema:

```text
raw
```

As sete tabelas de destino são:

```text
raw.agencias
raw.clientes
raw.colaborador_agencia
raw.colaboradores
raw.contas
raw.propostas_credito
raw.transacoes
```

A persistência principal do PostgreSQL utiliza um PersistentVolume/PersistentVolumeClaim de **2 GiB**.

## 10. Usuários e permissões PostgreSQL

O projeto utiliza diferentes usuários para separar responsabilidades.

### Usuário administrativo

```text
banvic
```

É utilizado para tarefas administrativas do banco.

### Usuário de ingestão

```text
banvic_ingest
```

É utilizado pelo pipeline para gravar os dados na camada `raw`.

A role de ingestão não é o usuário administrativo do banco.

O schema:

```text
raw
```

pertence ao usuário:

```text
banvic_ingest
```

As permissões do database e do schema são configuradas via Terraform.

O `PUBLIC` não recebe acesso ao database `banvic` nem ao schema `public`.

## 11. Secrets e credenciais

As credenciais são armazenadas em Kubernetes Secrets.

Os principais Secrets são:

```text
banvic-postgres-admin
banvic-postgres-ingest
banvic-airflow-ingest
airflow-metadata
airflow-api-static-secret
airflow-jwt-secret
airflow-admin-secret
airflow-fernet
airflow-filesystem
```

As senhas são geradas automaticamente pelo Terraform quando não são fornecidas explicitamente.

Variáveis sensíveis também são declaradas como `sensitive` no Terraform.

O repositório não contém senhas, tokens ou chaves privadas.

## 12. Acesso restrito às credenciais de ingestão

As credenciais utilizadas pela pipeline são disponibilizadas especificamente ao Scheduler.

No `values.yaml`, o Scheduler recebe:

```text
AIRFLOW_CONN_BANVIC_POSTGRES
TARGET_POSTGRES_HOST
TARGET_POSTGRES_PORT
TARGET_POSTGRES_DATABASE
TARGET_POSTGRES_USER
TARGET_POSTGRES_PASSWORD
```

Os demais componentes do Airflow não recebem essas variáveis de conexão da ingestão.

Os arquivos CSV também são montados somente no Scheduler:

```text
/opt/airflow/data/raw
```

com:

```text
readOnly: true
```

Essa separação reduz a superfície de acesso aos dados e às credenciais de ingestão.

## 13. Persistência e volumes

A infraestrutura utiliza volumes persistentes separados para:

### PostgreSQL

```text
banvic-postgres-pv
banvic-postgres-pvc
```

Capacidade:

```text
2 GiB
```

### Logs do Airflow

```text
airflow-logs-pv
airflow-logs-pvc
```

Capacidade:

```text
2 GiB
```

### Arquivos CSV

```text
banvic-raw-data-pv
banvic-raw-data-pvc
```

Os arquivos são montados no Scheduler em:

```text
/opt/airflow/data/raw
```

e o volume é somente leitura para o Airflow.

## 14. Infraestrutura como código

A infraestrutura principal está declarada em:

```text
infra/terraform/
```

O projeto separa o provisionamento em dois módulos:

```text
infra/terraform/cluster/
infra/terraform/platform/
```

### Cluster

O módulo `cluster` utiliza o provider Kind para criar o cluster Kubernetes:

```text
kind
```

com um node `control-plane`.

A imagem do node é fixada por versão e digest.

Também são configurados mounts do host para:

```text
data/raw
runtime/airflow-logs
runtime/postgres-data
```

### Platform

O módulo `platform` provisiona:

- namespace Kubernetes;
- PostgreSQL;
- Service PostgreSQL;
- PVCs e PVs;
- Jobs de inicialização do PostgreSQL;
- usuários e permissões;
- Kubernetes Secrets;
- imagem customizada do Airflow;
- Helm release do Airflow;
- Job de criação do usuário administrador do Airflow.

### Providers

As versões principais estão fixadas nos arquivos `.tf`:

```text
hashicorp/kubernetes = 3.2.1
hashicorp/helm       = 3.3.0
hashicorp/random     = 3.9.0
tehcyx/kind          = 0.11.0
```

Os arquivos `.terraform.lock.hcl` são versionados para manter o controle das versões dos providers.

## 15. Airflow

O Airflow utiliza:

```text
Apache Airflow 3.2.2
```

com:

```text
LocalExecutor
```

O Helm chart utilizado é:

```text
1.22.0
```

Componentes não necessários ao POC são desabilitados, incluindo:

```text
Redis
PgBouncer
Triggerer
StatsD
Flower
GitSync
```

O PostgreSQL interno do chart também é desabilitado:

```yaml
postgresql:
  enabled: false
```

O Airflow utiliza o database:

```text
airflow
```

para seus metadados.

Os dados da pipeline permanecem separados no database:

```text
banvic
```

## 16. Imagem customizada do Airflow

O Dockerfile utiliza como base:

```text
apache/airflow:3.2.2
```

A imagem instala:

```text
Meltano 4.2.2
```

em:

```text
/opt/meltano-venv
```

e instala os providers necessários do Airflow.

A imagem contém:

```text
dags/
meltano/meltano.yml
meltano/config/
```

Os arquivos CSV não são incluídos no Dockerfile.

A imagem é construída localmente e carregada no cluster Kind.

## 17. Bootstrap do ambiente

O script:

```text
scripts/bootstrap.ps1
```

automatiza o provisionamento rápido do ambiente.

Ele:

1. valida Docker, kubectl, Kind e Helm;
2. cria o cluster Kind caso necessário;
3. cria o namespace `banvic`;
4. cria ou reutiliza as credenciais do PostgreSQL;
5. aplica PostgreSQL, Service e PVC;
6. cria ou reutiliza o banco de metadados do Airflow;
7. cria ou reutiliza o API Secret do Airflow;
8. garante a existência do schema `raw`;
9. cria ou reutiliza a credencial do usuário admin;
10. constrói a imagem customizada do Airflow;
11. carrega a imagem no Kind;
12. configura o repositório Helm;
13. instala ou atualiza o Airflow;
14. cria o usuário admin caso ainda não exista.

### Execução

No PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

O script foi projetado para ser reutilizável, preservando Secrets existentes quando possível.

## 18. Verificação do ambiente

O script:

```text
scripts/verify.ps1
```

faz uma validação do ambiente sem modificar a infraestrutura.

Ele verifica:

- existência dos pods esperados;
- estado `Ready` dos containers;
- `Airflow parallelism`;
- presença da DAG `banvic_elt`;
- existência do schema `raw`;
- quantidade de registros nas sete tabelas;
- últimas execuções da DAG.

Executar:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

Uma execução válida termina com:

```text
=== Resultado ===
Verificação concluída.
```

## 19. Execução da DAG

A DAG está configurada para execução automática:

```text
schedule = 0 6 * * *
```

considerando o timezone:

```text
America/Sao_Paulo
```

A configuração:

```text
catchup = False
```

evita a criação automática de execuções atrasadas.

Também é possível executar a DAG manualmente pela interface do Airflow.

### Acesso ao Airflow

Execute:

```powershell
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace banvic
```

Depois acesse:

```text
http://localhost:8080
```

Usuário:

```text
admin
```

A senha fica armazenada no Secret:

```text
airflow-admin-secret
```

### Fluxo esperado

```text
7 FileSensors
      ↓
7 ingestões independentes
      ↓
summary
```

Cada ingestão aguarda seu respectivo arquivo antes de executar o Meltano.

## 20. Consultando os dados

Para acessar o PostgreSQL:

```powershell
kubectl exec -it deploy/banvic-postgres -n banvic -- psql -U banvic -d banvic
```

Exemplo:

```sql
SELECT COUNT(*) FROM raw.transacoes;
```

Resultado esperado no dataset utilizado neste POC:

```text
71999
```

Outra consulta útil:

```sql
SELECT
    'agencias' AS tabela,
    COUNT(*) AS registros
FROM raw.agencias
UNION ALL
SELECT 'clientes', COUNT(*) FROM raw.clientes
UNION ALL
SELECT 'colaborador_agencia', COUNT(*) FROM raw.colaborador_agencia
UNION ALL
SELECT 'colaboradores', COUNT(*) FROM raw.colaboradores
UNION ALL
SELECT 'contas', COUNT(*) FROM raw.contas
UNION ALL
SELECT 'propostas_credito', COUNT(*) FROM raw.propostas_credito
UNION ALL
SELECT 'transacoes', COUNT(*) FROM raw.transacoes
ORDER BY tabela;
```

## 21. Resultado da validação

O ambiente foi validado após a execução da infraestrutura e da DAG.

### Contagens validadas

| Tabela | Registros |
|---|---:|
| `raw.agencias` | 10 |
| `raw.clientes` | 998 |
| `raw.colaborador_agencia` | 100 |
| `raw.colaboradores` | 100 |
| `raw.contas` | 999 |
| `raw.propostas_credito` | 2.000 |
| `raw.transacoes` | 71.999 |

A DAG também foi executada com sucesso após a configuração final de recursos do Scheduler.

## 22. Modelo conceitual

O modelo conceitual completo é apresentado abaixo:

![Modelo Conceitual de Dados](docs/banvic_modelo_conceitual.png)

As principais entidades são:

```text
CLIENTES
CONTAS
TRANSACOES
PROPOSTAS_CREDITO
COLABORADORES
AGENCIAS
COLABORADOR_AGENCIA
```

### Principais chaves

```text
CLIENTES
  PK: cod_cliente

CONTAS
  PK: num_conta
  FK: cod_cliente
  FK: cod_agencia
  FK: cod_colaborador

TRANSACOES
  PK: cod_transacao
  FK: num_conta

PROPOSTAS_CREDITO
  PK: cod_proposta
  FK: cod_cliente
  FK: cod_colaborador

COLABORADORES
  PK: cod_colaborador

AGENCIAS
  PK: cod_agencia

COLABORADOR_AGENCIA
  PK composta: cod_colaborador + cod_agencia
  FK: cod_colaborador
  FK: cod_agencia
```

A `colaborador_agencia` é uma tabela associativa entre colaboradores e agências. Por isso, sua chave é representada pelo par:

```text
(cod_colaborador, cod_agencia)
```

A camada `raw` foi mantida simples para o objetivo do POC e não depende da criação de constraints relacionais para realizar a ingestão.

## 23. Observações sobre qualidade dos dados

As validações exploratórias realizadas durante o desenvolvimento encontraram alguns pontos relevantes no dataset:

- existe referência ao cliente `528` em dados transacionais/de crédito, embora esse cliente não esteja presente em `clientes.csv`;
- foram encontrados quatro emails duplicados na base de clientes.

Esses pontos foram preservados no `raw`, em vez de serem descartados ou corrigidos silenciosamente.

A decisão é coerente com a função da camada `raw`: preservar a origem e permitir que regras de qualidade, tratamento e modelagem sejam aplicadas em camadas posteriores.

## 24. Limitações e próximos passos

Este projeto é um POC, portanto algumas decisões foram deliberadamente simplificadas.

Possíveis evoluções:

- adicionar uma camada `staging` com dbt;
- criar modelos analíticos e dimensões/fatos;
- implementar testes de qualidade adicionais;
- adicionar validações de schema e tipos;
- implementar carga incremental quando a fonte disponibilizar uma coluna de alteração confiável;
- utilizar armazenamento externo para arquivos em ambientes produtivos;
- utilizar um executor distribuído caso o volume de dados cresça;
- adicionar observabilidade com métricas e dashboards;
- adicionar CI/CD para validação automática da infraestrutura, DAG e imagem;
- utilizar um Secret Manager externo em ambientes produtivos.

## 25. Reprodução rápida

### Pré-requisitos

```text
Docker Desktop
kubectl
Kind
Helm
Terraform >= 1.6.0
PowerShell
```

Clone o repositório:

```powershell
git clone https://github.com/vitorturcisn/banvic-data-engineering.git
cd banvic-data-engineering
```

### Opção 1 — Bootstrap simplificado

Execute:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

Depois valide:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

### Opção 2 — Terraform

Provisionamento do cluster:

```powershell
cd .\infra\terraform\cluster

terraform init
terraform validate
terraform plan
terraform apply
```

Depois:

```powershell
cd ..\platform

terraform init
terraform validate
terraform plan
terraform apply
```

O módulo `platform` utiliza o cluster Kind definido pelo módulo `cluster`.

### Acesso ao Airflow

```powershell
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace banvic
```

Abra:

```text
http://localhost:8080
```

## 26. Entregáveis principais

Os principais artefatos deste POC são:

```text
dags/banvic_elt.py
infra/airflow/
infra/kind/
infra/postgres/
infra/terraform/
meltano/
data/raw/
docs/
scripts/bootstrap.ps1
scripts/verify.ps1
README.md
1_GITHUB.txt
2_VIDEO.txt
```

## 27. Repositório

GitHub:

https://github.com/vitorturcisn/banvic-data-engineering
