$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$Namespace = "banvic"
$ClusterName = "banvic"
$AirflowRelease = "airflow"
$AirflowChartVersion = "1.22.0"

function Write-Step {
    param([string]$Message)

    Write-Host ""
    Write-Host "=== $Message ===" -ForegroundColor Cyan
}

function Assert-Command {
    param([string]$Command)

    if (-not (Get-Command $Command -ErrorAction SilentlyContinue)) {
        throw "Comando não encontrado: $Command"
    }
}

function New-RandomHex {
    param([int]$Bytes = 32)

    $buffer = New-Object byte[] $Bytes
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()

    try {
        $rng.GetBytes($buffer)
    }
    finally {
        $rng.Dispose()
    }

    return [BitConverter]::ToString($buffer).Replace("-", "")
}

function Get-SecretValue {
    param(
        [string]$SecretName,
        [string]$Key
    )

    $value = kubectl get secret $SecretName `
        --namespace $Namespace `
        --ignore-not-found=true `
        -o "jsonpath={.data.$Key}" 2>$null

    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($value)) {
        return $null
    }

    return [System.Text.Encoding]::UTF8.GetString(
        [System.Convert]::FromBase64String($value)
    )
}

Write-Step "Validando ferramentas"

Assert-Command "docker"
Assert-Command "kubectl"
Assert-Command "kind"
Assert-Command "helm"

Write-Host "Docker, kubectl, Kind e Helm encontrados." -ForegroundColor Green

Write-Step "Validando cluster Kind"

$clusterExists = kind get clusters 2>$null |
    Select-String "^$ClusterName$"

if (-not $clusterExists) {
    Write-Host "Criando cluster Kind '$ClusterName'..."

    kind create cluster `
        --name $ClusterName `
        --config ".\infra\kind\cluster.yaml"
}
else {
    Write-Host "Cluster '$ClusterName' já existe."
}

kubectl cluster-info --context "kind-$ClusterName"

Write-Step "Criando namespace"

kubectl apply -f ".\infra\namespace.yaml"

Write-Step "Configurando credenciais do PostgreSQL"

$postgresUser = Get-SecretValue `
    "banvic-postgres-secret" `
    "POSTGRES_USER"

$postgresPassword = Get-SecretValue `
    "banvic-postgres-secret" `
    "POSTGRES_PASSWORD"

$postgresDatabase = Get-SecretValue `
    "banvic-postgres-secret" `
    "POSTGRES_DB"

if (-not $postgresUser) {
    $postgresUser = "banvic"
}

if (-not $postgresDatabase) {
    $postgresDatabase = "banvic"
}

if (-not $postgresPassword) {
    Write-Host "Secret banvic-postgres-secret não existe. Gerando credencial..."

    $postgresPassword = New-RandomHex 32

    kubectl create secret generic banvic-postgres-secret `
        --namespace $Namespace `
        --from-literal="POSTGRES_USER=$postgresUser" `
        --from-literal="POSTGRES_PASSWORD=$postgresPassword" `
        --from-literal="POSTGRES_DB=$postgresDatabase"
}
else {
    Write-Host "Secret banvic-postgres-secret já existe. Mantendo configuração atual."
}

Write-Step "Subindo PostgreSQL"

kubectl apply -f ".\infra\postgres\pvc.yaml"
kubectl apply -f ".\infra\postgres\deployment.yaml"
kubectl apply -f ".\infra\postgres\service.yaml"

kubectl rollout status deployment/banvic-postgres `
    --namespace $Namespace `
    --timeout=5m

Write-Step "Preparando banco do Airflow"

# Recupera a connection existente, se houver.
$airflowConnection = Get-SecretValue `
    "airflow-metadata" `
    "connection"

if ($airflowConnection) {

    Write-Host "Secret airflow-metadata já existe. Mantendo configuração atual."

    # A connection string gerada pelo bootstrap usa senha hexadecimal.
    # Extraímos somente a senha para garantir que a role do PostgreSQL
    # permaneça sincronizada com o Secret.
    if ($airflowConnection -match '^postgresql(?:\+\w+)?://airflow:([^@]+)@') {
        $airflowDbPassword = $Matches[1]
    }
    else {
        throw "Não foi possível interpretar o Secret airflow-metadata. Formato inesperado."
    }
}
else {

    Write-Host "Secret airflow-metadata não existe. Gerando credencial..."

    $airflowDbPassword = New-RandomHex 32
}

Write-Host "Verificando role airflow..."

$airflowRoleExists = kubectl exec `
    -n $Namespace `
    deploy/banvic-postgres `
    -- psql `
    -U $postgresUser `
    -d postgres `
    -tAc "SELECT 1 FROM pg_roles WHERE rolname='airflow';"

if ($airflowRoleExists.Trim() -eq "1") {

    Write-Host "Role airflow já existe. Atualizando credencial..."

    kubectl exec `
        -n $Namespace `
        deploy/banvic-postgres `
        -- psql `
        -U $postgresUser `
        -d postgres `
        -v ON_ERROR_STOP=1 `
        -c "ALTER ROLE airflow WITH LOGIN PASSWORD '$airflowDbPassword';"
}
else {

    Write-Host "Criando role airflow..."

    kubectl exec `
        -n $Namespace `
        deploy/banvic-postgres `
        -- psql `
        -U $postgresUser `
        -d postgres `
        -v ON_ERROR_STOP=1 `
        -c "CREATE ROLE airflow LOGIN PASSWORD '$airflowDbPassword';"
}

Write-Host "Verificando database airflow..."

$dbExists = kubectl exec `
    -n $Namespace `
    deploy/banvic-postgres `
    -- psql `
    -U $postgresUser `
    -d postgres `
    -tAc "SELECT 1 FROM pg_database WHERE datname='airflow';"

if ($dbExists.Trim() -eq "1") {

    Write-Host "Database airflow já existe."

    kubectl exec `
        -n $Namespace `
        deploy/banvic-postgres `
        -- psql `
        -U $postgresUser `
        -d postgres `
        -v ON_ERROR_STOP=1 `
        -c "ALTER DATABASE airflow OWNER TO airflow;"
}
else {

    Write-Host "Criando database airflow..."

    kubectl exec `
        -n $Namespace `
        deploy/banvic-postgres `
        -- psql `
        -U $postgresUser `
        -d postgres `
        -v ON_ERROR_STOP=1 `
        -c "CREATE DATABASE airflow OWNER airflow;"
}

# Caso o Secret ainda não exista, cria a connection string.
if (-not $airflowConnection) {

    $airflowConnection = `
        "postgresql+psycopg://airflow:$airflowDbPassword@banvic-postgres.$Namespace.svc.cluster.local:5432/airflow"

    kubectl create secret generic airflow-metadata `
        --namespace $Namespace `
        --from-literal="connection=$airflowConnection"

    Write-Host "Secret airflow-metadata criado."
}

Write-Step "Criando API secret do Airflow"

$apiSecret = Get-SecretValue `
    "airflow-api-static-secret" `
    "api-secret-key"

if (-not $apiSecret) {

    Write-Host "API secret não existe. Gerando..."

    $apiSecret = New-RandomHex 32

    kubectl create secret generic airflow-api-static-secret `
        --namespace $Namespace `
        --from-literal="api-secret-key=$apiSecret"
}
else {
    Write-Host "airflow-api-static-secret já existe."
}

Write-Step "Criando schema raw"

kubectl exec `
    -n $Namespace `
    deploy/banvic-postgres `
    -- psql `
    -U $postgresUser `
    -d $postgresDatabase `
    -v ON_ERROR_STOP=1 `
    -c "CREATE SCHEMA IF NOT EXISTS raw;"

Write-Step "Preparando usuário admin do Airflow"

$adminPassword = Get-SecretValue `
    "airflow-admin-secret" `
    "password"

if (-not $adminPassword) {

    Write-Host "Secret airflow-admin-secret não existe. Gerando senha..."

    $adminPassword = New-RandomHex 32

    kubectl create secret generic airflow-admin-secret `
        --namespace $Namespace `
        --from-literal="username=admin" `
        --from-literal="password=$adminPassword" `
        --from-literal="email=admin@banvic.local"
}
else {
    Write-Host "Secret airflow-admin-secret já existe."
}

Write-Step "Construindo imagem do Airflow"

$valuesContent = Get-Content `
    ".\infra\airflow\values.yaml" `
    -Raw

if ($valuesContent -notmatch 'defaultAirflowRepository:\s*([^\r\n]+)') {
    throw "defaultAirflowRepository não encontrado em values.yaml."
}

$airflowRepository = $Matches[1].Trim()

if ($valuesContent -notmatch 'defaultAirflowTag:\s*"([^"]+)"') {
    throw "defaultAirflowTag não encontrado em values.yaml."
}

$airflowTag = $Matches[1]

$airflowImage = "${airflowRepository}:${airflowTag}"

Write-Host "Imagem do Airflow: $airflowImage"

docker build `
    -t $airflowImage `
    -f ".\infra\airflow\Dockerfile" `
    .

Write-Step "Carregando imagem no Kind"

kind load docker-image `
    $airflowImage `
    --name $ClusterName

Write-Step "Configurando Helm"

helm repo add `
    apache-airflow `
    https://airflow.apache.org `
    --force-update

helm repo update

Write-Step "Instalando/atualizando Airflow"

helm upgrade --install `
    $AirflowRelease `
    apache-airflow/airflow `
    --namespace $Namespace `
    --version $AirflowChartVersion `
    --values ".\infra\airflow\values.yaml" `
    --wait `
    --timeout 10m

Write-Step "Verificando criação dos pods"

kubectl get pods -n $Namespace

Write-Step "Criando usuário admin, se necessário"

$adminExists = kubectl exec `
    -n $Namespace `
    airflow-scheduler-0 `
    -c scheduler `
    -- airflow users list 2>$null |
    Select-String "admin@banvic.local"

if ($adminExists) {

    Write-Host "Usuário admin já existe."
}
else {

    Write-Host "Criando usuário admin..."

    kubectl exec `
        -n $Namespace `
        airflow-scheduler-0 `
        -c scheduler `
        -- airflow users create `
        --username admin `
        --firstname Admin `
        --lastname Banvic `
        --role Admin `
        --email admin@banvic.local `
        --password $adminPassword
}

Write-Step "Validação final"

kubectl get pods -n $Namespace

Write-Host ""
Write-Host "Ambiente BanVic preparado com sucesso." `
    -ForegroundColor Green

Write-Host ""
Write-Host "Airflow:"
Write-Host "kubectl port-forward svc/airflow-api-server 8080:8080 --namespace banvic"

Write-Host ""
Write-Host "Usuário Airflow: admin"
Write-Host "A senha está armazenada no Secret Kubernetes 'airflow-admin-secret'."