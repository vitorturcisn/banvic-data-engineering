$ErrorActionPreference = "Stop"

$Namespace = "banvic"

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
        exit 1
    }

    Write-Host "[OK] $Message" -ForegroundColor Green
}

Write-Step "Pods"

kubectl get pods -n $Namespace

$pods = kubectl get pods -n $Namespace -o json | ConvertFrom-Json

$expectedPods = @(
    "airflow-api-server",
    "airflow-dag-processor",
    "airflow-scheduler",
    "banvic-postgres"
)

foreach ($prefix in $expectedPods) {
    $pod = $pods.items |
        Where-Object { $_.metadata.name -like "$prefix*" } |
        Select-Object -First 1

    Assert-Ok `
        ($null -ne $pod) `
        "Pod $prefix existe"

    $ready = $pod.status.containerStatuses |
        Where-Object { $_.ready -eq $false }

    Assert-Ok `
        ($null -eq $ready) `
        "Pod $($pod.metadata.name) está Ready"
}

Write-Step "Airflow parallelism"

$parallelism = kubectl exec -n $Namespace airflow-scheduler-0 -c scheduler -- `
    airflow config get-value core parallelism

Assert-Ok `
    ($parallelism.Trim() -eq "2") `
    "Airflow parallelism = 2"

Write-Step "DAG"

$dag = kubectl exec -n $Namespace airflow-scheduler-0 -c scheduler -- `
    airflow dags list

$dagText = $dag -join "`n"

Assert-Ok `
    ($dagText -match "banvic_elt") `
    "DAG banvic_elt encontrada"

Write-Step "PostgreSQL"

$rawSchema = kubectl exec -n $Namespace deploy/banvic-postgres -- `
    psql -U banvic -d banvic -tAc `
    "SELECT 1 FROM information_schema.schemata WHERE schema_name='raw';"

Assert-Ok `
    ($rawSchema.Trim() -eq "1") `
    "Schema raw existe"

Write-Step "Contagem das tabelas"

$query = @"
SELECT 'agencias' AS tabela, COUNT(*) AS registros FROM raw.agencias
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
"@

kubectl exec -n $Namespace deploy/banvic-postgres -- `
    psql -U banvic -d banvic -c $query

Write-Step "Últimas execuções da DAG"

kubectl exec -n $Namespace airflow-scheduler-0 -c scheduler -- `
    airflow dags list-runs banvic_elt

Write-Step "Resultado"

Write-Host "Verificação concluída." -ForegroundColor Green
