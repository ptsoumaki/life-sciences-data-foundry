nextflow.enable.dsl=2

params.raw_input = "s3://multiomics-raw-prod/*.fastq"
params.outdir    = "s3://multiomics-processed-prod/qc/"

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

workflow {
    input_ch = Channel.fromPath(params.raw_input)
    FASTQC(input_ch)
}