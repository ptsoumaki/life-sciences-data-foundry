nextflow.enable.dsl=2

process PROVENANCE_MANIFEST {
    tag "Generating 21 CFR Part 11 provenance manifest"
    publishDir "${params.outdir}", mode: 'copy'

    input:
    path input_files
    path ingestion_summary

    output:
    path "provenance_manifest.json", emit: manifest

    script:
    """
    python "${projectDir}/provenance.py" \\
        --input-files ${input_files} \\
        --summary-file ${ingestion_summary} \\
        --output-manifest "provenance_manifest.json" \\
        --pipeline-version "${workflow.manifest.version}" \\
        --workflow-session-id "${workflow.sessionId}" \\
        --delta-log-dir "${params.outdir}/omop/gold/measurement"
    """

    stub:
    """
    cat << 'EOF' > provenance_manifest.json
    {
      "manifest_version": "1.0.0",
      "pipeline_name": "life-sciences-data-foundry-pipeline",
      "pipeline_version": "0.4.0",
      "workflow_session_id": "stub-session-000",
      "timestamp": "2026-09-20T00:00:00Z",
      "compliance": {
        "regulatory_standard": "FDA 21 CFR Part 11 (§11.10, §11.50)",
        "hash_algorithm": "SHA-256",
        "audit_trail_status": "VALIDATED"
      },
      "inputs": [],
      "containers": {
        "fastqc": "quay.io/biocontainers/fastqc:0.12.1--hdfd78af_0",
        "bcftools": "quay.io/biocontainers/bcftools:1.19--h8b25389_1",
        "multiqc": "quay.io/biocontainers/multiqc:1.21--pyhdfd78af_0"
      },
      "ingestion_summary": {
        "status": "SUCCESS",
        "variant_count": 42,
        "omop_measurement_count": 42,
        "execution_mode": "stub"
      },
      "target_delta_version": 0,
      "manifest_sha256": "9275d8454a48db88b67be2b7b202755489bc783d13b8879b48b9d3eef333f569"
    }
    EOF
    """
}
