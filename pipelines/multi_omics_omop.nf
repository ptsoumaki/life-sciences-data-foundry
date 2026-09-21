nextflow.enable.dsl=2

/*
========================================================================================
    Life Sciences Data Foundry — Multi-Omics to OMOP Normalization Pipeline
========================================================================================
    Orchestrates:
      1. Raw FASTQ quality control (FASTQC)
      2. VCF variant filtering, statistics, and annotation extraction (BCFTOOLS)
      3. Aggregated multi-tool quality report generation (MULTIQC)
      4. PySpark Medallion ingestion into OMOP CDM v5.4 MEASUREMENT Delta Lake (OMOP_INGEST)
      5. FDA 21 CFR Part 11 cryptographic provenance manifest generation (PROVENANCE_MANIFEST)
========================================================================================
*/

include { FASTQC }                   from './modules/fastqc.nf'
include { BCFTOOLS_ANNOTATE_FILTER } from './modules/bcftools.nf'
include { MULTIQC }                  from './modules/multiqc.nf'
include { OMOP_INGEST }              from './modules/omop_ingest.nf'
include { PROVENANCE_MANIFEST }      from './modules/provenance.nf'

workflow {
    // Validate target environment parameter
    if (!params.environment) {
        error("ENVIRONMENT is not set. Export ENVIRONMENT or set params.environment in nextflow.config before running Nextflow.")
    }

    log.info """
    =======================================================================
     Life Sciences Data Foundry — Multi-Omics OMOP Pipeline v${workflow.manifest.version}
    =======================================================================
     Environment        : ${params.environment}
     AWS Region         : ${params.aws_region}
     Raw FASTQ Pattern  : ${params.raw_fastq}
     Raw VCF Pattern    : ${params.raw_vcf}
     Output Directory   : ${params.outdir}
     MultiQC Directory  : ${params.multiqc_dir}
     Data Mode          : ${params.data_mode}
     Save Delta Tables  : ${params.save_delta}
    =======================================================================
    """.stripIndent()

    // Channel creation with graceful empty handling
    fastq_ch = Channel.fromPath(params.raw_fastq)
        .ifEmpty { error("No input FASTQ files found matching: ${params.raw_fastq}") }

    vcf_ch = Channel.fromPath(params.raw_vcf)
        .ifEmpty { error("No input VCF files found matching: ${params.raw_vcf}") }

    // 1. Raw sequencing quality control
    FASTQC(fastq_ch)

    // 2. VCF variant filtering, statistics, and ClinVar annotation
    BCFTOOLS_ANNOTATE_FILTER(vcf_ch)

    // 3. Multi-tool QC metric aggregation
    MULTIQC(
        FASTQC.out.zip.collect(),
        BCFTOOLS_ANNOTATE_FILTER.out.stats.collect()
    )

    // 4. PySpark Medallion Delta Lake ingestion into OMOP CDM v5.4
    OMOP_INGEST(BCFTOOLS_ANNOTATE_FILTER.out.vcf)

    // 5. Generate FDA 21 CFR Part 11 cryptographic provenance manifest
    PROVENANCE_MANIFEST(
        fastq_ch.mix(vcf_ch).collect(),
        OMOP_INGEST.out.summary.collect()
    )
}
