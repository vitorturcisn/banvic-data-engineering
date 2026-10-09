
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Namespace = "banvic"
$DagId = "banvic_elt"

$ExpectedCounts = [ordered]@{
    agencias             = 10
    clientes             = 998
    colaborador_agencia  = 100
    colaboradores        = 100
    contas               = 999
    propostas_credito    = 2000
    transacoes           = 71999
}

$ExpectedTasks = @(
    "wait_for_clientes",
    "ingest_clientes",
    "wait_for_contas",
    "ingest_contas",
    "wait_for_transacoes",
    "ingest_transacoes",
    "wait_for_propostas_credito",
    "ingest_propostas_credito",
    "wait_for_colaboradores",
    "ingest_colaboradores",
    "wait_for_agencias",
    "ingest_agencias",
    "wait_for_colaborador_agencia",
    "ingest_colaborador_agencia",
    "summarize_ingestion"
)

function Write-Step {
    param([string]$Message)

    Write-Host ""
    Write-Host "=== $Message ===" -ForegroundColor Cyan
}

function Assert-Ok {
    param(
        [bool]$Condition,
        [string]$Message
    )

    if (-not $Condition) {
        Write-Host "[FAIL] $Message" -ForegroundColor Red
        throw $Message
    }

    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Invoke-Kubectl {
    param(
        [string[]]$Arguments
    )

    & kubectl @Arguments

    if ($LASTEXITCODE -ne 0) {
        throw "kubectl falhou."
    }
}

Write-Step "Pods"

Invoke-Kubectl @(
    "get",
    "pods",
    "-n",
    $Namespace
)

$podsJson = kubectl get pods -n $Namespace -o json

if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível consultar os pods do namespace $Namespace."
}

try {
    $pods = $podsJson | ConvertFrom-Json
}
catch {
    throw "Não foi possível interpretar o JSON dos pods: $($_.Exception.Message)"
}

$expectedPodPrefixes = @(
    "airflow-api-server",
    "airflow-dag-processor",
    "airflow-scheduler",
    "banvic-postgres"
)

foreach ($prefix in $expectedPodPrefixes) {

    $pod = $pods.items |
        Where-Object {
            $_.metadata.name -like "$prefix*"
        } |
        Select-Object -First 1

    Assert-Ok `
        ($null -ne $pod) `
        "Pod $prefix existe"

    $notReady = @(
        $pod.status.containerStatuses |
            Where-Object {
                $_.ready -ne $true
            }
    )

    Assert-Ok `
        ($notReady.Count -eq 0) `
        "Pod $($pod.metadata.name) está Ready"
}

Write-Step "Airflow parallelism"

$parallelism = kubectl exec `
    -n $Namespace `
    airflow-scheduler-0 `
    -c scheduler `
    -- airflow config get-value core parallelism

if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível consultar o parallelism do Airflow."
}

Assert-Ok `
    ($parallelism.Trim() -eq "2") `
    "Airflow parallelism = 2"

Write-Step "DAG"

$dagList = kubectl exec `
    -n $Namespace `
    airflow-scheduler-0 `
    -c scheduler `
    -- airflow dags list

if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível listar as DAGs do Airflow."
}

$dagText = $dagList -join "`n"

Assert-Ok `
    ($dagText -match "(^|\s)banvic_elt(\s|$)") `
    "DAG banvic_elt encontrada"

Write-Step "Arquivo da DAG"

$dagPath = Join-Path $ProjectRoot "dags\banvic_elt.py"

Assert-Ok `
    (Test-Path $dagPath) `
    "Arquivo dags\banvic_elt.py existe"

$dagSource = Get-Content $dagPath -Raw

Assert-Ok `
    ($dagSource -match 'schedule\s*=\s*"0 6 \* \* \*"') `
    "Schedule configurado como 0 6 * * *"

Assert-Ok `
    ($dagSource -match 'max_active_runs\s*=\s*1') `
    "max_active_runs = 1"

Assert-Ok `
    ($dagSource -match 'max_active_tasks\s*=\s*2') `
    "max_active_tasks = 2"

Assert-Ok `
    ($dagSource -match '"retries"\s*:\s*2') `
    "retries = 2"

Write-Step "Tasks da DAG"

$taskList = kubectl exec `
    -n $Namespace `
    airflow-scheduler-0 `
    -c scheduler `
    -- airflow tasks list $DagId

if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível listar as tasks da DAG $DagId."
}

$taskText = $taskList -join "`n"

foreach ($taskId in $ExpectedTasks) {

    Assert-Ok `
        ($taskText -match "(^|\s)$taskId(\s|$)") `
        "Task $taskId encontrada"
}

Write-Step "Quantidade esperada de tasks"

Assert-Ok `
    ($ExpectedTasks.Count -eq 15) `
    "DAG possui 15 tasks esperadas"

Write-Step "PostgreSQL / schema raw"

$rawSchema = kubectl exec `
    -n $Namespace `
    deploy/banvic-postgres `
    -- psql `
    -U banvic_ingest `
    -d banvic `
    -tAc `
    "SELECT 1 FROM information_schema.schemata WHERE schema_name='raw';"

if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível consultar o schema raw."
}

Assert-Ok `
    ($rawSchema.Trim() -eq "1") `
    "Schema raw existe"

Write-Step "Contagem das tabelas"

$query = @"
SELECT 'agencias' AS tabela, COUNT(*) AS registros
FROM raw.agencias

UNION ALL

SELECT 'clientes', COUNT(*)
FROM raw.clientes

UNION ALL

SELECT 'colaborador_agencia', COUNT(*)
FROM raw.colaborador_agencia

UNION ALL

SELECT 'colaboradores', COUNT(*)
FROM raw.colaboradores

UNION ALL

SELECT 'contas', COUNT(*)
FROM raw.contas

UNION ALL

SELECT 'propostas_credito', COUNT(*)
FROM raw.propostas_credito

UNION ALL

SELECT 'transacoes', COUNT(*)
FROM raw.transacoes

ORDER BY tabela;
"@

$queryResult = kubectl exec `
    -n $Namespace `
    deploy/banvic-postgres `
    -- psql `
    -U banvic_ingest `
    -d banvic `
    -tA `
    -F "|" `
    -c $query

if ($LASTEXITCODE -ne 0) {
    throw "Falha ao consultar as contagens das tabelas no PostgreSQL."
}

$rows = @()

foreach ($line in $queryResult) {

    $parts = $line.Trim().Split("|")

    if ($parts.Count -eq 2) {

        $rows += [PSCustomObject]@{
            tabela    = $parts[0]
            registros = [int]$parts[1]
        }
    }
}

foreach ($tableName in $ExpectedCounts.Keys) {

    $row = $rows |
        Where-Object {
            $_.tabela -eq $tableName
        } |
        Select-Object -First 1

    Assert-Ok `
        ($null -ne $row) `
        "Tabela raw.$tableName existe"

    Assert-Ok `
        ($row.registros -eq $ExpectedCounts[$tableName]) `
        "raw.$tableName possui $($ExpectedCounts[$tableName]) registros"
}

Write-Step "Últimas execuções da DAG"

# Captura a saída inteira porque o Airflow pode emitir logs
# de inicialização antes do JSON solicitado.
$dagRunsOutput = kubectl exec `
    -n $Namespace `
    airflow-scheduler-0 `
    -c scheduler `
    -- airflow dags list-runs $DagId --output json 2>&1

$dagRunsExitCode = $LASTEXITCODE

if ($dagRunsExitCode -ne 0) {
    throw "Não foi possível consultar as execuções da DAG."
}

$dagRunsText = ($dagRunsOutput | ForEach-Object {
    $_.ToString()
}) -join "`n"

# Localiza o array JSON que começa com os objetos de execução,
# ignorando mensagens de log anteriores à resposta.
$jsonMatch = [regex]::Match(
    $dagRunsText,
    '(?s)(\[\s*\{\s*"dag_id"\s*:.*\])\s*$'
)

if (-not $jsonMatch.Success) {
    Write-Host "Saída recebida do Airflow:" -ForegroundColor Yellow
    Write-Host $dagRunsText
    throw "Não foi possível localizar o JSON das execuções da DAG."
}

try {
    $dagRuns = $jsonMatch.Groups[1].Value | ConvertFrom-Json
}
catch {
    throw "Não foi possível interpretar o JSON das execuções da DAG: $($_.Exception.Message)"
}

$runs = @($dagRuns)

Assert-Ok `
    ($runs.Count -gt 0) `
    "Existe pelo menos uma execução da DAG"

# O Airflow pode retornar logical_date vazio em algumas
# execuções manuais; nesses casos, usa-se start_date.
$latestRun = $runs |
    Sort-Object {
        if (
            -not [string]::IsNullOrWhiteSpace(
                [string]$_.logical_date
            )
        ) {
            [datetimeoffset]$_.logical_date
        }
        elseif (
            -not [string]::IsNullOrWhiteSpace(
                [string]$_.start_date
            )
        ) {
            [datetimeoffset]$_.start_date
        }
        else {
            [datetimeoffset]::MinValue
        }
    } -Descending |
    Select-Object -First 1

Assert-Ok `
    ($null -ne $latestRun) `
    "Foi possível identificar a última execução da DAG"

Write-Host ""
Write-Host "Última execução:" -ForegroundColor Yellow
Write-Host "Run ID : $($latestRun.run_id)"
Write-Host "Estado : $($latestRun.state)"

Assert-Ok `
    ($latestRun.state -eq "success") `
    "Última execução da DAG banvic_elt = success"

Write-Step "Resultado"

Write-Host ""
Write-Host "Verificação concluída com sucesso." -ForegroundColor Green
