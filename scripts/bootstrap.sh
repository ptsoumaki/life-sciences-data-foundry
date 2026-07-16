#!/usr/bin/env bash
# Bootstrap environment for local development: source .env and export TF_VAR_* vars
set -euo pipefail

# Dynamically find the repo root directory relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

ENV_FILE="$REPO_ROOT/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo ".env file not found at $ENV_FILE. Copy from .env.example and edit values." >&2
  exit 1
fi

echo "Loading .env from $ENV_FILE..."
set -a
source "$ENV_FILE"
set +a

export TF_VAR_environment="$ENVIRONMENT"
export TF_VAR_aws_region="$AWS_REGION"

echo "Exported TF_VAR_environment=$TF_VAR_environment TF_VAR_aws_region=$TF_VAR_aws_region"