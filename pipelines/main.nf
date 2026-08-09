nextflow.enable.dsl=2

params.raw_input = "s3://multiomics-raw-${params.environment}/*.fastq"
params.outdir    = "s3://multiomics-processed-${params.environment}/qc/"

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
    // Require the environment param to be set in the shell or via nextflow.config
    if (!params.environment) {
        error("ENVIRONMENT is not set. Export ENVIRONMENT or set params.environment in nextflow.config before running Nextflow.")
    }
    
    input_ch = Channel.fromPath(params.raw_input, checkIfExists: true)
        .ifEmpty { error("No input files found matching: ${params.raw_input}") }
    FASTQC(input_ch)
}