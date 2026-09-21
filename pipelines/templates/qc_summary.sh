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
    for zip_file in "$1"/*_fastqc.zip; do
        if [ -f "$zip_file" ]; then
            echo "--- $(basename "$zip_file") ---"
            unzip -p "$zip_file" "*/summary.txt" 2>/dev/null | grep -E "PASS|WARN|FAIL" || true
        fi
    done
else
    echo "No QC reports directory supplied. Dry-run mode completed."
fi
