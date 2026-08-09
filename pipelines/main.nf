nextflow.enable.dsl=2

include { FASTQC } from './modules/fastqc.nf'

params.raw_input = "s3://life-sciences-platform-raw-${params.environment}/*.fastq"
params.outdir    = "s3://life-sciences-platform-processed-${params.environment}/qc/"

workflow {
    // Require the environment param to be set in the shell or via nextflow.config
    if (!params.environment) {
        error("ENVIRONMENT is not set. Export ENVIRONMENT or set params.environment in nextflow.config before running Nextflow.")
    }
    
    input_ch = Channel.fromPath(params.raw_input, checkIfExists: true)
        .ifEmpty { error("No input files found matching: ${params.raw_input}") }
        
    FASTQC(input_ch)
}