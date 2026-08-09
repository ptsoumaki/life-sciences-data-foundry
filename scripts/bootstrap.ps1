<#
.SYNOPSIS
    Bootstrap PowerShell script to import .env values and set environment variables for Terraform and Nextflow.
.DESCRIPTION
    Reads .env from the repository root, safely parses key=value pairs, and exports process-level environment variables.
#>
$ErrorActionPreference = "Stop"

# Dynamically calculate repository root directory (one level up from /scripts)
$ScriptDir = $PSScriptRoot
$RepoRoot = Split-Path -Parent $ScriptDir
$EnvFile = Join-Path $RepoRoot ".env"

if (-not (Test-Path -Path $EnvFile)) {
    Write-Host "[WARNING] .env file not found at: $EnvFile" -ForegroundColor Yellow
    Write-Host "[INFO] Copying template from .env.example to .env..." -ForegroundColor Cyan
    $ExampleFile = Join-Path $RepoRoot ".env.example"
    if (Test-Path -Path $ExampleFile) {
        Copy-Item -Path $ExampleFile -Destination $EnvFile
        Write-Host "[SUCCESS] Created .env from .env.example. Please review default parameters." -ForegroundColor Green
    } else {
        Write-Error ".env.example not found at $ExampleFile."; exit 1
    }
}

Write-Host "[INFO] Loading environment parameters from: $EnvFile" -ForegroundColor Cyan

Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#")) { return }
    if ($line -like "*=*") {
        $parts = $line -split "=", 2
        $name = $parts[0].Trim()
        $value = $parts[1].Trim().Trim('"').Trim("'")
        
        [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        
        if ($name -eq "ENVIRONMENT") {
            [System.Environment]::SetEnvironmentVariable("TF_VAR_environment", $value, "Process")
        }
        if ($name -eq "AWS_REGION") {
            [System.Environment]::SetEnvironmentVariable("TF_VAR_aws_region", $value, "Process")
        }
    }
}

$envName = [System.Environment]::GetEnvironmentVariable("ENVIRONMENT", "Process")
$regName = [System.Environment]::GetEnvironmentVariable("AWS_REGION", "Process")
Write-Host "[SUCCESS] Loaded environment variables into process scope." -ForegroundColor Green
Write-Host "          ENVIRONMENT        = $envName" -ForegroundColor Gray
Write-Host "          AWS_REGION         = $regName" -ForegroundColor Gray
Write-Host "          TF_VAR_environment = $envName" -ForegroundColor Gray
Write-Host "          TF_VAR_aws_region   = $regName" -ForegroundColor Gray