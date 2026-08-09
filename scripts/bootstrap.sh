#!/usr/bin/env bash
# Bootstrap environment for local development: source .env and export TF_VAR_* vars
set -euo pipefail

# Dynamically find the repo root directory relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$REPO_ROOT/.env"
EXAMPLE_FILE="$REPO_ROOT/.env.example"

if [ ! -f "$ENV_FILE" ]; then
  echo "[WARNING] .env file not found at $ENV_FILE." >&2
  if [ -f "$EXAMPLE_FILE" ]; then
    echo "[INFO] Copying template from .env.example to .env..."
    cp "$EXAMPLE_FILE" "$ENV_FILE"
    echo "[SUCCESS] Created .env from .env.example."
  else
    echo "[ERROR] Neither .env nor .env.example found." >&2
    exit 1
  fi
fi

echo "[INFO] Loading environment parameters from $ENV_FILE..."

# Parse .env line by line to safely ignore comments and quotes
while IFS= read -r line || [ -n "$line" ]; do
  clean_line="$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  
  if [[ -z "$clean_line" || "$clean_line" =~ ^# ]]; then
    continue
  fi

  if [[ "$clean_line" =~ = ]]; then
    key="${clean_line%%=*}"
    val="${clean_line#*=}"
    
    val="${val%\"}"
    val="${val#\"}"
    val="${val%\'}"
    val="${val#\'}"

    export "$key"="$val"
  fi
done < "$ENV_FILE"

export TF_VAR_environment="${ENVIRONMENT:-dev}"
export TF_VAR_aws_region="${AWS_REGION:-eu-west-1}"

echo "[SUCCESS] Exported environment variables:"
echo "          ENVIRONMENT        = ${ENVIRONMENT:-dev}"
echo "          AWS_REGION         = ${AWS_REGION:-eu-west-1}"
echo "          TF_VAR_environment = ${TF_VAR_environment}"
echo "          TF_VAR_aws_region   = ${TF_VAR_aws_region}"