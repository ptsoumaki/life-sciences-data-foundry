nextflow.enable.dsl=2

process FASTQC {
    tag "QC processing on ${fastq.baseName}"
    publishDir "${params.outdir}", mode: 'copy'
    
    input:
    path fastq
    
    output:
    path "*_fastqc.{zip,html}", emit: qc_reports

    script:
    """
    fastqc --quiet ${fastq}
    """
}
