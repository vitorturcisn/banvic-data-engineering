$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$ClusterDir = Join-Path $ProjectRoot "infra\terraform\cluster"
$PlatformDir = Join-Path $ProjectRoot "infra\terraform\platform"

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

function Invoke-Terraform {
    param(
        [string]$WorkingDirectory,
        [string[]]$Arguments
    )

    Push-Location $WorkingDirectory

    try {
        & terraform @Arguments

        if ($LASTEXITCODE -ne 0) {
            throw "Terraform falhou no diretório '$WorkingDirectory'."
        }
    }
    finally {
        Pop-Location
    }
}

Write-Step "Validando ferramentas"

Assert-Command "docker"
Assert-Command "kubectl"
Assert-Command "kind"
Assert-Command "helm"
Assert-Command "terraform"

Write-Host "Docker, kubectl, Kind, Helm e Terraform encontrados." -ForegroundColor Green

Write-Step "Validando Docker"

docker info *> $null

if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop não está disponível. Inicie o Docker Desktop e execute novamente."
}

Write-Host "Docker disponível." -ForegroundColor Green

Write-Step "Provisionando cluster Kind com Terraform"

Invoke-Terraform `
    -WorkingDirectory $ClusterDir `
    -Arguments @(
        "init"
    )

Invoke-Terraform `
    -WorkingDirectory $ClusterDir `
    -Arguments @(
        "validate"
    )

Invoke-Terraform `
    -WorkingDirectory $ClusterDir `
    -Arguments @(
        "apply",
        "-auto-approve"
    )

Write-Step "Validando cluster Kubernetes"

kubectl cluster-info --context "kind-banvic"

if ($LASTEXITCODE -ne 0) {
    throw "Não foi possível acessar o cluster Kind 'banvic'."
}

Write-Host "Cluster Kind disponível." -ForegroundColor Green

Write-Step "Provisionando plataforma BanVic com Terraform"

Invoke-Terraform `
    -WorkingDirectory $PlatformDir `
    -Arguments @(
        "init"
    )

Invoke-Terraform `
    -WorkingDirectory $PlatformDir `
    -Arguments @(
        "validate"
    )

Invoke-Terraform `
    -WorkingDirectory $PlatformDir `
    -Arguments @(
        "apply",
        "-auto-approve"
    )

Write-Step "Aguardando PostgreSQL"

kubectl rollout status `
    deployment/banvic-postgres `
    --namespace banvic `
    --timeout=5m

Write-Step "Aguardando Airflow API Server"

kubectl rollout status `
    deployment/airflow-api-server `
    --namespace banvic `
    --timeout=10m

Write-Step "Aguardando Airflow DAG Processor"

kubectl rollout status `
    deployment/airflow-dag-processor `
    --namespace banvic `
    --timeout=10m

Write-Step "Aguardando Airflow Scheduler"

kubectl rollout status `
    statefulset/airflow-scheduler `
    --namespace banvic `
    --timeout=10m

Write-Step "Estado dos pods"

kubectl get pods -n banvic

Write-Step "PersistentVolumeClaims"

kubectl get pvc -n banvic

Write-Step "Resultado"

Write-Host ""
Write-Host "Ambiente BanVic preparado com sucesso." -ForegroundColor Green

Write-Host ""
Write-Host "Airflow:"
Write-Host "kubectl port-forward svc/airflow-api-server 8080:8080 --namespace banvic"

Write-Host ""
Write-Host "URL:"
Write-Host "http://localhost:8080"

Write-Host ""
Write-Host "Para executar a validação:"
Write-Host "powershell -ExecutionPolicy Bypass -File .\scripts\verify.ps1"