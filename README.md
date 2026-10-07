# BanVic Data Engineering POC

Proof of Concept de Engenharia de Dados para ingestÃ£o, orquestraÃ§Ã£o e validaÃ§Ã£o dos dados de um ERP bancÃ¡rio fictÃ­cio (BanVic).

O projeto implementa uma pipeline ELT executada em Kubernetes/Kind, com Apache Airflow para orquestraÃ§Ã£o, Meltano para ingestÃ£o dos arquivos CSV e PostgreSQL como camada de destino (`raw`).

A infraestrutura Ã© declarada com Terraform, enquanto Helm Ã© utilizado pelo Terraform para instalar o Apache Airflow.

---

## 1. VisÃ£o geral

O objetivo deste POC Ã© demonstrar uma soluÃ§Ã£o reprodutÃ­vel de Engenharia de Dados, cobrindo:

- provisionamento de um cluster Kubernetes local com Kind;
- provisionamento da plataforma com Terraform;
- execuÃ§Ã£o do Apache Airflow em Kubernetes;
- ingestÃ£o dos arquivos CSV usando Meltano;
- carga dos dados em PostgreSQL na camada `raw`;
- validaÃ§Ã£o da existÃªncia dos arquivos antes da ingestÃ£o;
- validaÃ§Ã£o da quantidade de registros apÃ³s a carga;
- retries e timeout nas tarefas de ingestÃ£o;
- controle de concorrÃªncia da DAG;
- carga idempotente utilizando snapshot completo;
- armazenamento de credenciais em Kubernetes Secrets;
- persistÃªncia dos dados do PostgreSQL;
- persistÃªncia dos logs do Airflow;
- scripts PowerShell para bootstrap e verificaÃ§Ã£o do ambiente.

---

## 2. Arquitetura

![Arquitetura da Plataforma do POC](docs/banvic_arquitetura.png)

### Fluxo da plataforma

```text
                       Terraform
                           â”‚
          â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”´â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
          â”‚                                 â”‚
          â–¼                                 â–¼
      Kind Cluster                    Platform Kubernetes
                                          â”‚
                           â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
                           â”‚              â”‚              â”‚
                           â–¼              â–¼              â–¼
                        Airflow       PostgreSQL       PVCs
                           â”‚              â”‚
                           â”‚              â””â”€â”€ raw
                           â”‚
                           â–¼
                     DAG banvic_elt
                           â”‚
          â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
          â”‚                â”‚                 â”‚
          â–¼                â–¼                 â–¼
   FileSensor #1 ... FileSensor #7     ...
          â”‚                â”‚
          â–¼                â–¼
   ingest_table #1 ... ingest_table #7
          â”‚
          â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                           â–¼
                  summarize_ingestion
```

---

## 3. Tecnologias

| Tecnologia | Uso |
|---|---|
| Docker | ConstruÃ§Ã£o da imagem customizada do Airflow |
| Kind | Cluster Kubernetes local |
| Kubernetes | ExecuÃ§Ã£o e gerenciamento dos componentes |
| Terraform | Provisionamento da infraestrutura |
| Helm | InstalaÃ§Ã£o do Apache Airflow |
| Apache Airflow 3.2.2 | OrquestraÃ§Ã£o da pipeline |
| Meltano 4.2.2 | ExtraÃ§Ã£o e carregamento dos CSVs |
| tap-csv | Extractor dos arquivos CSV |
| target-postgres | Loader para PostgreSQL |
| PostgreSQL 16.15 | Armazenamento dos dados |
| Python | ImplementaÃ§Ã£o da DAG e validaÃ§Ãµes |
| PowerShell | AutomaÃ§Ã£o do bootstrap e verificaÃ§Ã£o |

### VersÃµes principais

```text
Terraform: >= 1.6.0
Airflow: 3.2.2
Apache Airflow Helm Chart: 1.22.0
Meltano: 4.2.2
PostgreSQL: 16.15-alpine3.24
Kind node image: kindest/node:v1.35.8
Terraform provider kind: 0.11.0
Terraform provider kubernetes: 3.2.1
Terraform provider helm: 3.3.0
Terraform provider random: 3.9.0
```

---

## 4. Estrutura do projeto

```text
banvic-data-engineering/
â”‚
â”œâ”€â”€ dags/
â”‚   â””â”€â”€ banvic_elt.py
â”‚
â”œâ”€â”€ data/
â”‚   â””â”€â”€ raw/
â”‚       â”œâ”€â”€ agencias.csv
â”‚       â”œâ”€â”€ clientes.csv
â”‚       â”œâ”€â”€ colaborador_agencia.csv
â”‚       â”œâ”€â”€ colaboradores.csv
â”‚       â”œâ”€â”€ contas.csv
â”‚       â”œâ”€â”€ propostas_credito.csv
â”‚       â””â”€â”€ transacoes.csv
â”‚
â”œâ”€â”€ docs/
â”‚   â”œâ”€â”€ banvic_arquitetura.png
â”‚   â””â”€â”€ banvic_modelo_conceitual.png
â”‚
â”œâ”€â”€ airflow/
â”‚   â”œâ”€â”€ Dockerfile
â”‚   â”œâ”€â”€ requirements.txt
â”‚   â””â”€â”€ values.yaml
â”‚
â”œâ”€â”€ kind/
â”‚   â””â”€â”€ cluster.yaml
â”‚
â”œâ”€â”€ meltano/
â”‚   â”œâ”€â”€ config/
â”‚   â”‚   â””â”€â”€ csv_files_definition.json
â”‚   â”œâ”€â”€ meltano.yml
â”‚   â””â”€â”€ *_lock.yml
â”‚
â”œâ”€â”€ terraform/
â”‚   â”œâ”€â”€ cluster/
â”‚   â”‚   â”œâ”€â”€ .terraform.lock.hcl
â”‚   â”‚   â”œâ”€â”€ main.tf
â”‚   â”‚   â”œâ”€â”€ outputs.tf
â”‚   â”‚   â”œâ”€â”€ providers.tf
â”‚   â”‚   â”œâ”€â”€ variables.tf
â”‚   â”‚   â””â”€â”€ versions.tf
â”‚   â”‚
â”‚   â””â”€â”€ platform/
â”‚       â”œâ”€â”€ .terraform.lock.hcl
â”‚       â”œâ”€â”€ airflow.tf
â”‚       â”œâ”€â”€ namespace.tf
â”‚       â”œâ”€â”€ postgres.tf
â”‚       â”œâ”€â”€ postgres-init.tf
â”‚       â”œâ”€â”€ providers.tf
â”‚       â”œâ”€â”€ random.tf
â”‚       â”œâ”€â”€ secrets.tf
â”‚       â”œâ”€â”€ storage.tf
â”‚       â”œâ”€â”€ variables.tf
â”‚       â””â”€â”€ versions.tf
â”‚
â”œâ”€â”€ scripts/
â”‚   â”œâ”€â”€ bootstrap.ps1
â”‚   â””â”€â”€ verify.ps1
â”‚
â”œâ”€â”€ .dockerignore
â”œâ”€â”€ .gitignore
â”œâ”€â”€ 1_GITHUB.txt
â””â”€â”€ README.md
```

> `kind/` pode ser removido apÃ³s esta limpeza, pois a configuraÃ§Ã£o do cluster Kind estÃ¡ atualmente declarada diretamente em `terraform/cluster/main.tf`.

---

## 5. Dados de entrada

O POC utiliza sete arquivos CSV fornecidos pelo desafio:

| Arquivo | Destino | Registros |
|---|---|---:|
| `agencias.csv` | `raw.agencias` | 10 |
| `clientes.csv` | `raw.clientes` | 998 |
| `colaborador_agencia.csv` | `raw.colaborador_agencia` | 100 |
| `colaboradores.csv` | `raw.colaboradores` | 100 |
| `contas.csv` | `raw.contas` | 999 |
| `propostas_credito.csv` | `raw.propostas_credito` | 2.000 |
| `transacoes.csv` | `raw.transacoes` | 71.999 |

Os arquivos permanecem no repositÃ³rio em `data/raw/`.

Eles **nÃ£o sÃ£o incorporados Ã  imagem Docker do Airflow**.

O `.dockerignore` exclui:

```text
data/raw/
```

Durante a execuÃ§Ã£o, os CSVs sÃ£o disponibilizados ao Scheduler atravÃ©s de um PersistentVolume/PersistentVolumeClaim.

---

## 6. EstratÃ©gia de ingestÃ£o

A ingestÃ£o segue o fluxo:

```text
CSV
 â”‚
 â–¼
tap-csv
 â”‚
 â–¼
target-postgres
 â”‚
 â–¼
PostgreSQL
 â”‚
 â–¼
raw
```

### Extract

O `tap-csv` lÃª os arquivos definidos em:

```text
meltano/config/csv_files_definition.json
```

A configuraÃ§Ã£o define:

- arquivo de origem;
- caminho relativo;
- chave do stream;
- delimitador `,`;
- encoding UTF-8;
- `strict: true`.

A associaÃ§Ã£o `colaborador_agencia` utiliza uma chave composta:

```text
cod_colaborador
cod_agencia
```

### Load

O `target-postgres` utiliza:

```yaml
default_target_schema: raw
load_method: overwrite
validate_records: true
activate_version: false
```

A estratÃ©gia `overwrite` foi escolhida porque os arquivos representam uma fotografia completa da origem.

Cada execuÃ§Ã£o recompÃµe as tabelas `raw` com o snapshot atual.

Isso fornece comportamento idempotente para o conjunto de arquivos utilizado no POC.

---

## 7. OrquestraÃ§Ã£o com Airflow

A DAG principal Ã©:

```text
banvic_elt
```

Arquivo:

```text
dags/banvic_elt.py
```

### Agendamento

```text
0 6 * * *
```

A DAG utiliza:

```text
catchup = False
max_active_runs = 1
max_active_tasks = 2
```

O `default_args` utiliza:

```text
retries = 2
retry_delay = 1 minuto
```

As tarefas de ingestÃ£o utilizam:

```text
execution_timeout = 15 minutos
```

### Fluxo real da DAG

Cada arquivo possui um `FileSensor` prÃ³prio:

```text
wait_for_clientes
        â”‚
        â–¼
ingest_clientes

wait_for_contas
        â”‚
        â–¼
ingest_contas

wait_for_transacoes
        â”‚
        â–¼
ingest_transacoes

wait_for_propostas_credito
        â”‚
        â–¼
ingest_propostas_credito

wait_for_colaboradores
        â”‚
        â–¼
ingest_colaboradores

wait_for_agencias
        â”‚
        â–¼
ingest_agencias

wait_for_colaborador_agencia
        â”‚
        â–¼
ingest_colaborador_agencia
```

Ao final:

```text
ingestÃµes
    â”‚
    â–¼
summarize_ingestion
```

A DAG possui 15 tarefas:

```text
7 FileSensors
7 tarefas de ingestÃ£o
1 tarefa de resumo
```

---

## 8. FileSensors

Cada tabela possui um `FileSensor` configurado com:

```text
fs_conn_id = fs_default
poke_interval = 30 segundos
timeout = 600 segundos
mode = reschedule
```

O `filepath` utiliza apenas o nome do arquivo, por exemplo:

```text
clientes.csv
```

A conexÃ£o `fs_default` aponta para:

```text
/opt/airflow/data/raw
```

Somente o Scheduler recebe o volume dos arquivos e as credenciais necessÃ¡rias Ã  ingestÃ£o.

---

## 9. ValidaÃ§Ãµes da pipeline

Antes da ingestÃ£o, cada tarefa:

1. verifica a existÃªncia do arquivo;
2. calcula a quantidade de registros da origem;
3. valida a presenÃ§a das variÃ¡veis de conexÃ£o;
4. executa o Meltano.

ApÃ³s a ingestÃ£o:

1. consulta o PostgreSQL atravÃ©s de `PostgresHook`;
2. conta os registros da tabela `raw`;
3. compara origem e destino;
4. falha caso os quantitativos sejam diferentes.

Exemplo:

```text
source transacoes      = 71.999
destination transacoes = 71.999
```

---

## 10. PostgreSQL

O PostgreSQL roda no namespace:

```text
banvic
```

ServiÃ§o:

```text
banvic-postgres
```

Porta:

```text
5432
```

O servidor possui:

```text
database banvic
database airflow
```

O banco `banvic` contÃ©m:

```text
schema raw
```

Com as tabelas:

```text
raw.agencias
raw.clientes
raw.colaborador_agencia
raw.colaboradores
raw.contas
raw.propostas_credito
raw.transacoes
```

O PostgreSQL utiliza um PersistentVolume/PersistentVolumeClaim de:

```text
2 GiB
```

A persistÃªncia utiliza:

```text
/var/local/banvic-postgres-data
```

montado no node Kind.

---

## 11. Modelo conceitual

![Modelo Conceitual de Dados](docs/banvic_modelo_conceitual.png)

Principais entidades:

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
  PK composta:
    cod_colaborador
    cod_agencia
```

A camada `raw` nÃ£o depende da criaÃ§Ã£o de constraints relacionais para realizar a ingestÃ£o.

---

## 12. Infraestrutura como cÃ³digo

A infraestrutura atual utiliza Terraform como fonte de verdade.

### Cluster

DiretÃ³rio:

```text
terraform/cluster/
```

O Terraform cria o cluster Kind utilizando:

```text
kindest/node:v1.35.8
```

com digest fixado.

O node recebe os seguintes mounts:

```text
data/raw
runtime/airflow-logs
runtime/postgres-data
```

### Plataforma

DiretÃ³rio:

```text
terraform/platform/
```

A plataforma provisiona:

```text
namespace
PostgreSQL
Service PostgreSQL
PersistentVolumes
PersistentVolumeClaims
Secrets
Job de inicializaÃ§Ã£o do PostgreSQL
Airflow via Helm
Job de criaÃ§Ã£o do usuÃ¡rio administrativo do Airflow
```

### Airflow

A imagem customizada Ã©:

```text
banvic-airflow:2.1.0
```

O Terraform calcula hashes do:

```text
Dockerfile
requirements.txt
DAG
meltano.yml
csv_files_definition.json
```

para reconstruir a imagem quando componentes relevantes forem alterados.

A imagem Ã© carregada diretamente no cluster Kind.

---

## 13. PersistÃªncia

HÃ¡ trÃªs volumes persistentes principais:

```text
PostgreSQL:
2 GiB

Airflow logs:
2 GiB

CSV raw:
10 MiB
```

### Airflow logs

Os logs sÃ£o persistidos em:

```text
/var/local/airflow-logs
```

atravÃ©s do PVC:

```text
airflow-logs-pvc
```

### CSVs

Os arquivos de origem ficam fora da imagem Docker e sÃ£o montados no Scheduler em:

```text
/opt/airflow/data/raw
```

O volume Ã© montado como somente leitura.

---

## 14. Secrets e credenciais

As credenciais sÃ£o geradas e gerenciadas pelo Terraform utilizando Kubernetes Secrets.

Principais Secrets:

```text
banvic-postgres-admin
banvic-postgres-ingest
airflow-metadata
banvic-airflow-ingest
airflow-api-static-secret
airflow-jwt-secret
airflow-admin-secret
airflow-fernet
airflow-filesystem
```

O usuÃ¡rio usado pela pipeline Ã©:

```text
banvic_ingest
```

Ele nÃ£o Ã© o usuÃ¡rio administrativo do PostgreSQL.

O Scheduler recebe as credenciais do usuÃ¡rio de ingestÃ£o atravÃ©s do Secret:

```text
banvic-airflow-ingest
```

A DAG utiliza a conexÃ£o:

```text
banvic_postgres
```

por meio do `PostgresHook`.

As senhas nÃ£o sÃ£o armazenadas no Git.

O `.gitignore` exclui:

```text
.env
.env.*
*.env
*.key
*.pem
```

TambÃ©m sÃ£o excluÃ­dos arquivos de estado e artefatos locais do Terraform.

---

## 15. Controle de acesso

O PostgreSQL Ã© inicializado pelo Terraform atravÃ©s do arquivo:

```text
terraform/platform/postgres-init.tf
```

SÃ£o criados:

```text
airflow
banvic_ingest
```

O acesso pÃºblico aos databases Ã© revogado.

O usuÃ¡rio `banvic_ingest` recebe:

```text
CONNECT
TEMPORARY
USAGE
CREATE
```

no contexto necessÃ¡rio Ã  ingestÃ£o.

O schema:

```text
raw
```

Ã© propriedade de:

```text
banvic_ingest
```

---

## 16. Bootstrap

O provisionamento completo Ã© feito pelo:

```text
scripts/bootstrap.ps1
```

O script:

1. valida Docker, kubectl, Kind, Helm e Terraform;
2. inicializa e aplica o Terraform do cluster;
3. inicializa e aplica o Terraform da plataforma;
4. aguarda os recursos principais;
5. exibe o estado dos pods;
6. informa como acessar o Airflow.

### ExecuÃ§Ã£o

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

O processo Ã© idempotente do ponto de vista da infraestrutura declarada: recursos existentes sÃ£o mantidos pelo estado do Terraform e modificaÃ§Ãµes necessÃ¡rias sÃ£o aplicadas pelo Terraform.

---

## 17. VerificaÃ§Ã£o

O script:

```text
scripts/verify.ps1
```

faz uma validaÃ§Ã£o sem modificar a infraestrutura.

Ele verifica:

```text
pods principais
containers Ready
Airflow parallelism
existÃªncia da DAG
existÃªncia do schema raw
contagem das sete tabelas
Ãºltimas execuÃ§Ãµes da DAG
```

### ExecuÃ§Ã£o

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

---

## 18. Acesso ao Airflow

Execute:

```powershell
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace banvic
```

Depois acesse:

```text
http://localhost:8080
```

UsuÃ¡rio:

```text
admin
```

A senha Ã© armazenada no Secret:

```text
airflow-admin-secret
```

---

## 19. ExecuÃ§Ã£o manual da DAG

A DAG pode ser executada pela interface do Airflow ou atravÃ©s do CLI:

```powershell
kubectl exec -n banvic airflow-scheduler-0 -c scheduler -- `
  airflow dags trigger banvic_elt
```

Para consultar as execuÃ§Ãµes:

```powershell
kubectl exec -n banvic airflow-scheduler-0 -c scheduler -- `
  airflow dags list-runs banvic_elt
```

Para consultar as tasks de uma execuÃ§Ã£o:

```powershell
kubectl exec -n banvic airflow-scheduler-0 -c scheduler -- `
  airflow tasks states-for-dag-run `
  banvic_elt `
  manual__YYYY-MM-DDTHH:MM:SS.ssssss+00:00
```

---

## 20. Consultando os dados

Para acessar o PostgreSQL:

```powershell
kubectl exec -it deploy/banvic-postgres -n banvic -- `
  psql -U banvic -d banvic
```

Exemplo:

```sql
SELECT COUNT(*)
FROM raw.transacoes;
```

Resultado esperado:

```text
71999
```

Consulta completa:

```sql
SELECT
    'agencias' AS tabela,
    COUNT(*) AS registros
FROM raw.agencias

UNION ALL

SELECT
    'clientes',
    COUNT(*)
FROM raw.clientes

UNION ALL

SELECT
    'colaborador_agencia',
    COUNT(*)
FROM raw.colaborador_agencia

UNION ALL

SELECT
    'colaboradores',
    COUNT(*)
FROM raw.colaboradores

UNION ALL

SELECT
    'contas',
    COUNT(*)
FROM raw.contas

UNION ALL

SELECT
    'propostas_credito',
    COUNT(*)
FROM raw.propostas_credito

UNION ALL

SELECT
    'transacoes',
    COUNT(*)
FROM raw.transacoes

ORDER BY tabela;
```

---

## 21. Resultado da validaÃ§Ã£o

A soluÃ§Ã£o foi executada e validada em ambiente Kind.

### Componentes principais

```text
airflow-api-server       Running / Ready
airflow-dag-processor    Running / Ready
airflow-scheduler        Running / Ready
banvic-postgres          Running / Ready
```

### Paralelismo

```text
Airflow parallelism = 2
```

### Resultado da DAG

A execuÃ§Ã£o validada apresentou:

```text
15 / 15 tasks = success
```

DistribuÃ­das em:

```text
7 FileSensors
7 ingestÃµes
1 summarize_ingestion
```

### Quantidade de registros

| Tabela | Registros |
|---|---:|
| `raw.agencias` | 10 |
| `raw.clientes` | 998 |
| `raw.colaborador_agencia` | 100 |
| `raw.colaboradores` | 100 |
| `raw.contas` | 999 |
| `raw.propostas_credito` | 2.000 |
| `raw.transacoes` | 71.999 |

---

## 22. Qualidade dos dados

Durante as validaÃ§Ãµes exploratÃ³rias foram observados alguns pontos do dataset:

- existe referÃªncia ao cliente `528` em dados transacionais/de crÃ©dito, embora esse cliente nÃ£o esteja presente em `clientes.csv`;
- existem quatro emails duplicados na base de clientes.

Esses registros foram preservados na camada `raw`.

A decisÃ£o Ã© intencional: a camada `raw` mantÃ©m os dados da origem sem correÃ§Ãµes silenciosas. Regras de qualidade, tratamento e modelagem podem ser aplicadas em camadas posteriores.

---

## 23. LimitaÃ§Ãµes e prÃ³ximos passos

Este projeto Ã© um POC e algumas decisÃµes foram deliberadamente simplificadas.

PossÃ­veis evoluÃ§Ãµes:

- adicionar uma camada `staging` com dbt;
- criar modelos dimensionais e fatos;
- implementar testes de qualidade mais completos;
- adicionar validaÃ§Ãµes de schema;
- separar fisicamente os databases de metadata e dados;
- utilizar armazenamento externo para os arquivos de origem;
- implementar carga incremental;
- utilizar executor distribuÃ­do para maiores volumes;
- adicionar observabilidade com mÃ©tricas e dashboards;
- implementar CI/CD;
- utilizar Secret Manager externo em ambiente produtivo.

---

## 24. ReproduÃ§Ã£o completa

### PrÃ©-requisitos

```text
Docker Desktop
kubectl
Kind
Helm
Terraform >= 1.6
PowerShell
Git
```

Clone o repositÃ³rio:

```powershell
git clone https://github.com/vitorturcisn/banvic-data-engineering.git
cd banvic-data-engineering
```

Execute:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
```

Depois valide:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1
```

Acesse o Airflow:

```powershell
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace banvic
```

Abra:

```text
http://localhost:8080
```

---

## 25. EntregÃ¡veis principais

```text
dags/banvic_elt.py

airflow/
  Dockerfile
  requirements.txt
  values.yaml

meltano/
  meltano.yml
  config/
  *_lock.yml

terraform/
  cluster/
  platform/

data/raw/

docs/

scripts/
  bootstrap.ps1
  verify.ps1

README.md
```
