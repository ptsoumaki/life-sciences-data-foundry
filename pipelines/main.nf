nextflow.enable.dsl=2

params.raw_input = "s3://life-sciences-platform-raw-${params.environment}/*.fastq"
params.outdir    = "s3://life-sciences-platform-processed-${params.environment}/qc/"

// Enforce strict environment parameter validation
if (!params.environment) {
    error("ENVIRONMENT is not set. Export ENVIRONMENT or set params.environment in nextflow.config before running Nextflow.")
}

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
    input_ch = Channel.fromPath(params.raw_input, checkIfExists: true)
        .ifEmpty { error("No input files found matching: ${params.raw_input}") }
    FASTQC(input_ch)
}