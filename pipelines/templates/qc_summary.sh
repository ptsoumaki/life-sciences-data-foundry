#!/usr/bin/env bash
# Workflow execution script template for summarizing FastQC reports
set -euo pipefail

echo "================================================="
echo " Quality Control Summary Execution Template "
echo " Environment: ${ENVIRONMENT:-dev}"
echo "================================================="

# Extract pass/fail metrics from FASTQC summary output
if [ -d "${1:-}" ]; then
    echo "Processing QC reports in directory: $1"
    grep -E "PASS|WARN|FAIL" "$1"/*.fastqc.zip 2>/dev/null || true
else
    echo "No QC reports directory supplied. Dry-run mode completed."
fi
