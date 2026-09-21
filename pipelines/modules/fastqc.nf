nextflow.enable.dsl=2

process FASTQC {
    tag "QC processing on ${fastq.baseName}"
    publishDir "${params.outdir}/qc", mode: 'copy'
    
    input:
    path fastq
    
    output:
    path "*_fastqc.{zip,html}", emit: qc_reports
    path "*_fastqc.zip", emit: zip
    path "*_fastqc.html", emit: html

    script:
    """
    fastqc --quiet "${fastq}"
    """

    stub:
    """
    touch "${fastq.baseName}_fastqc.zip"
    touch "${fastq.baseName}_fastqc.html"
    """
}
