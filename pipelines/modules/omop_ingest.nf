nextflow.enable.dsl=2

process OMOP_INGEST {
    tag "OMOP CDM ingestion on ${vcf.baseName}"
    publishDir "${params.outdir}/omop", mode: 'copy'

    input:
    path vcf

    output:
    path "${vcf.baseName}_ingestion_summary.json", emit: summary

    script:
    """
    python "${projectDir}/ingest_omop.py" \\
        --vcf "${vcf}" \\
        --output-dir "${params.outdir}/omop" \\
        --mode "${params.data_mode}" \\
        --summary-out "${vcf.baseName}_ingestion_summary.json"
    """

    stub:
    """
    cat << EOF > ${vcf.baseName}_ingestion_summary.json
    {
      "status": "SUCCESS",
      "vcf_file": "${vcf.name}",
      "variant_count": 42,
      "omop_measurement_count": 42,
      "write_mode": "append",
      "output_dir": "${params.outdir}/omop",
      "silver_table_path": "${params.outdir}/omop/silver/genomic_variants",
      "gold_table_path": "${params.outdir}/omop/gold/measurement",
      "target_delta_version": 0,
      "execution_mode": "stub",
      "timestamp": "2026-09-20T00:00:00Z"
    }
    EOF
    """
}
