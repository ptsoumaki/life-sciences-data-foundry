<#
Bootstrap PowerShell script to import .env values and set environment variables for Terraform and Nextflow
#>
# Dynamically calculate the repository root directory (one level up from /scripts)
$ScriptDir = $PSScriptRoot
$RepoRoot = Split-Path -Parent $ScriptDir
$EnvFile = Join-Path $RepoRoot ".env"

if (-not (Test-Path -Path $EnvFile)) {
    Write-Error ".env file not found at $EnvFile. Copy from .env.example and edit values."; exit 1
}

Write-Output "Loading .env from $EnvFile..."

Get-Content $EnvFile | ForEach-Object {
    if ($_ -match '^\s*#') { return }
    if ($_ -match '=') {
        $parts = $_ -split '='; $name = $parts[0].Trim(); $value = $parts[1].Trim()
        [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        if ($name -eq 'ENVIRONMENT') { [System.Environment]::SetEnvironmentVariable('TF_VAR_environment', $value, 'Process') }
        if ($name -eq 'AWS_REGION') { [System.Environment]::SetEnvironmentVariable('TF_VAR_aws_region', $value, 'Process') }
    }
}
Write-Output "Environment variables loaded into process. Remember to restart shells if you need them globally."