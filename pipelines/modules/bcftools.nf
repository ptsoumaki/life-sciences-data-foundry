nextflow.enable.dsl=2

process BCFTOOLS_ANNOTATE_FILTER {
    tag "Variant filtering & stats on ${vcf.baseName}"
    publishDir "${params.outdir}/variants", mode: 'copy'

    input:
    path vcf

    output:
    path "${vcf.baseName}_filtered.vcf", emit: vcf
    path "${vcf.baseName}_bcftools_stats.txt", emit: stats

    script:
    """
    bcftools view -O v -o "${vcf.baseName}_filtered.vcf" "${vcf}"
    bcftools stats "${vcf.baseName}_filtered.vcf" > "${vcf.baseName}_bcftools_stats.txt"
    """

    stub:
    """
    touch "${vcf.baseName}_filtered.vcf"
    touch "${vcf.baseName}_bcftools_stats.txt"
    """
}
