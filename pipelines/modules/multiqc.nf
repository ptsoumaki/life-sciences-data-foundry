nextflow.enable.dsl=2

process MULTIQC {
    tag "Aggregating multi-omics QC reports"
    publishDir "${params.multiqc_dir ?: "${params.outdir}/multiqc"}", mode: 'copy'

    input:
    path fastqc_files
    path bcftools_stats

    output:
    path "multiqc_report.html", emit: report
    path "multiqc_data", emit: data

    script:
    """
    multiqc . -n multiqc_report.html
    """

    stub:
    """
    touch multiqc_report.html
    mkdir -p multiqc_data
    touch multiqc_data/multiqc_general_stats.txt
    """
}
