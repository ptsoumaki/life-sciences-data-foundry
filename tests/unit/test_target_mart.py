"""
Unit tests for Target-to-Phenotype Evidence Mart and Discovery Lakehouse Engine (discovery/target_mart.py).
"""

import json
import os
import shutil
import tempfile

import pyarrow.parquet as pq
import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

from discovery.target_mart import (
    TARGET_EVIDENCE_SCHEMA,
    TargetEvidenceMart,
    validate_and_quarantine_target_records,
)
from medallion.quarantine import (
    QUARANTINE_TABLE_TARGETS,
    ClinicalFailureCode,
    QuarantineDeltaWriter,
)


@pytest.fixture
def target_discovery_synthetic_data(spark: SparkSession):
    """
    Generates synthetic OMOP CDM v5.4 cohort, condition occurrences, and genomic measurement records.
    Population: 10 subjects (IDs 1 through 10).
    """
    # 1. Cohort
    cohort_schema = StructType(
        [
            StructField("cohort_definition_id", LongType(), False),
            StructField("subject_id", LongType(), False),
            StructField("cohort_start_date", StringType(), False),
            StructField("cohort_end_date", StringType(), False),
        ]
    )
    cohort_rows = [(1001, i, "2023-01-01", "2024-01-01") for i in range(1, 11)]
    df_cohort = spark.createDataFrame(cohort_rows, cohort_schema)

    # 2. Genomic Measurements (ClinVar variants)
    # Subjects 1, 2, 3 have EGFR variants; Subjects 1, 4 have BRCA1 variants
    meas_schema = StructType(
        [
            StructField("measurement_id", LongType(), False),
            StructField("person_id", LongType(), False),
            StructField("measurement_concept_id", IntegerType(), False),
            StructField("value_as_concept_id", IntegerType(), False),
            StructField("value_as_number", DoubleType(), True),
            StructField("value_source_value", StringType(), False),
        ]
    )
    meas_rows = [
        # EGFR variants (measurement_concept_id 35917873 = Genomic variant)
        # value_source_value format: chrom:pos:ref:alt:id:gene_symbol:clinvar_sig
        (101, 1, 35917873, 4181412, None, "chr7:55181378:C:T:rs121434568:EGFR:Pathogenic"),
        (
            102,
            1,
            35917873,
            4181412,
            None,
            "chr7:55181380:G:A:rs121434569:EGFR:Pathogenic",
        ),  # 2nd mutation for subject 1
        (103, 2, 35917873, 4181412, None, "chr7:55181378:C:T:rs121434568:EGFR:Pathogenic"),
        (104, 3, 35917873, 36768280, None, "chr7:55181378:C:T:rs121434568:EGFR:Likely_pathogenic"),
        # BRCA1 variants
        (105, 1, 35917873, 4181412, None, "chr17:43044295:G:A:rs80357906:BRCA1:Pathogenic"),
        (106, 4, 35917873, 4181412, None, "chr17:43044295:G:A:rs80357906:BRCA1:Pathogenic"),
        # Quantitative lab measurements (e.g. glucose, HbA1c)
        (201, 1, 3004410, 0, 8.5, "HbA1c_elevated"),
        (202, 2, 3004410, 0, 7.8, "HbA1c_elevated"),
        (203, 5, 3004410, 0, 5.2, "HbA1c_normal"),
        (204, 6, 3004410, 0, 5.4, "HbA1c_normal"),
    ]
    df_measurement = spark.createDataFrame(meas_rows, meas_schema)

    # 3. Conditions (SNOMED concept 254637 = Lung cancer, 201826 = Type 2 diabetes)
    cond_schema = StructType(
        [
            StructField("condition_occurrence_id", LongType(), False),
            StructField("person_id", LongType(), False),
            StructField("condition_concept_id", LongType(), False),
            StructField("condition_start_date", StringType(), False),
        ]
    )
    cond_rows = [
        # Lung cancer (254637): Present in subjects 1, 2 (carriers) and 5, 6 (non-carriers)
        (1, 1, 254637, "2023-02-01"),
        (2, 2, 254637, "2023-02-15"),
        (3, 5, 254637, "2023-03-01"),
        (4, 6, 254637, "2023-03-10"),
        # Type 2 diabetes (201826): Present in subjects 1, 7, 8
        (5, 1, 201826, "2023-01-10"),
        (6, 7, 201826, "2023-04-01"),
        (7, 8, 201826, "2023-04-15"),
    ]
    df_condition = spark.createDataFrame(cond_rows, cond_schema)

    return {
        "cohort": df_cohort,
        "measurement": df_measurement,
        "condition": df_condition,
    }


def test_extract_target_variant_carriers(spark: SparkSession, target_discovery_synthetic_data):
    """Verifies variant extraction, token parsing, and carrier aggregation."""
    mart = TargetEvidenceMart(spark)
    df_meas = target_discovery_synthetic_data["measurement"]
    df_cohort = target_discovery_synthetic_data["cohort"]

    df_carriers = mart.extract_target_variant_carriers(df_meas, df_cohort=df_cohort)
    rows = df_carriers.collect()

    assert len(rows) > 0

    # Find EGFR carrier rows
    egfr_rows = [r for r in rows if r["target_gene_symbol"] == "EGFR"]
    egfr_person_ids = {r["person_id"] for r in egfr_rows}
    assert egfr_person_ids == {1, 2, 3}

    # Subject 1 has 2 variants in EGFR
    sub1_egfr = next(r for r in egfr_rows if r["person_id"] == 1)
    assert sub1_egfr["variant_count"] == 2
    assert sub1_egfr["is_carrier"] == 1
    assert sub1_egfr["has_pathogenic_variant"] == 1

    # Find BRCA1 carrier rows
    brca1_rows = [r for r in rows if r["target_gene_symbol"] == "BRCA1"]
    brca1_person_ids = {r["person_id"] for r in brca1_rows}
    assert brca1_person_ids == {1, 4}


def test_extract_phenotype_diagnoses(spark: SparkSession, target_discovery_synthetic_data):
    """Verifies distinct condition diagnoses extraction within cohort population."""
    mart = TargetEvidenceMart(spark)
    df_cond = target_discovery_synthetic_data["condition"]
    df_cohort = target_discovery_synthetic_data["cohort"]

    df_pheno = mart.extract_phenotype_diagnoses(df_cond, df_cohort=df_cohort)
    rows = df_pheno.collect()

    assert len(rows) == 7
    concepts = {r["disease_concept_id"] for r in rows}
    assert concepts == {254637, 201826}


def test_target_evidence_mart_odds_ratio_and_statistics(
    spark: SparkSession, target_discovery_synthetic_data
):
    """
    Verifies 2x2 contingency matrix construction, Haldane-Anscombe Odds Ratio,
    and asymptotic statistical precision against analytical derivation.
    """
    mart = TargetEvidenceMart(spark)
    df_cohort = target_discovery_synthetic_data["cohort"]
    df_cond = target_discovery_synthetic_data["condition"]
    df_meas = target_discovery_synthetic_data["measurement"]

    df_mart = mart.build_target_evidence_mart(df_cohort, df_cond, df_meas)

    # Check schema conformance
    for expected_field in TARGET_EVIDENCE_SCHEMA.fields:
        assert expected_field.name in df_mart.columns

    rows = df_mart.collect()
    assert len(rows) > 0

    # Inspect EGFR x Lung Cancer (254637):
    # Total Cohort N = 10
    # EGFR carriers: {1, 2, 3} -> Total carriers = 3
    # Lung Cancer cases: {1, 2, 5, 6} -> Total cases = 4
    # Carrier cases (a): {1, 2} = 2
    # Carrier controls (b): total_carriers - a = 3 - 2 = 1 (subject 3)
    # Non-carrier cases (c): total_cases - a = 4 - 2 = 2 (subjects 5, 6)
    # Non-carrier controls (d): N - (a + b + c) = 10 - 5 = 5 (subjects 4, 7, 8, 9, 10)
    egfr_lung = next(
        r for r in rows if r["target_gene_symbol"] == "EGFR" and r["disease_concept_id"] == 254637
    )

    assert egfr_lung["total_cohort_size"] == 10
    assert egfr_lung["carrier_cases"] == 2
    assert egfr_lung["carrier_controls"] == 1
    assert egfr_lung["non_carrier_cases"] == 2
    assert egfr_lung["non_carrier_controls"] == 5

    # Haldane-Anscombe derivation:
    # a_tilde = 2.5, b_tilde = 1.5, c_tilde = 2.5, d_tilde = 5.5
    # OR = (2.5 * 5.5) / (1.5 * 2.5) = 13.75 / 3.75 = 3.6667
    expected_or = round((2.5 * 5.5) / (1.5 * 2.5), 4)
    assert pytest.approx(egfr_lung["odds_ratio"], abs=0.01) == expected_or

    # Log OR = ln(3.6667) ≈ 1.2993
    # SE = sqrt(1/2.5 + 1/1.5 + 1/2.5 + 1/5.5) = sqrt(0.4 + 0.6667 + 0.4 + 0.1818) = sqrt(1.6485) ≈ 1.2839
    assert egfr_lung["odds_ratio_ci_lower"] < egfr_lung["odds_ratio"]
    assert egfr_lung["odds_ratio_ci_upper"] > egfr_lung["odds_ratio"]
    assert 0.0 <= egfr_lung["p_value"] <= 1.0

    # Target Mutation Burden for EGFR:
    # 4 mutations across 3 carriers = 4 / 3 ≈ 1.3333
    assert pytest.approx(egfr_lung["target_mutation_burden"], abs=0.05) == 1.3333

    # Tractability score must be bounded in [0.0, 1.0]
    assert 0.0 <= egfr_lung["target_tractability_score"] <= 1.0

    # Evidence tier must be one of the defined tiers
    assert egfr_lung["evidence_tier"] in {
        "TIER_1_VALIDATED",
        "TIER_2_CANDIDATE",
        "TIER_3_EXPLORATORY",
    }


def test_empty_cohort_and_inputs(spark: SparkSession):
    """Verifies that empty cohort inputs return typed empty DataFrames without runtime error."""
    mart = TargetEvidenceMart(spark)
    empty_cohort = spark.createDataFrame(
        [], StructType([StructField("subject_id", LongType(), False)])
    )
    empty_cond = spark.createDataFrame(
        [],
        StructType(
            [
                StructField("person_id", LongType(), False),
                StructField("condition_concept_id", LongType(), False),
            ]
        ),
    )
    empty_meas = spark.createDataFrame(
        [],
        StructType(
            [
                StructField("person_id", LongType(), False),
                StructField("measurement_concept_id", IntegerType(), False),
                StructField("value_source_value", StringType(), False),
            ]
        ),
    )

    df_res = mart.build_target_evidence_mart(empty_cohort, empty_cond, empty_meas)
    assert df_res.limit(1).count() == 0
    assert df_res.columns == [f.name for f in TARGET_EVIDENCE_SCHEMA.fields]


def test_target_contract_validation_and_quarantine_routing(spark: SparkSession):
    """
    Verifies that rows breaching declarative GxP contract invariants are quarantined
    into dead-letter sinks with failure code TARGET_CONTRACT_VIOLATION and zero data loss.
    """
    test_rows = [
        # 1. Compliant row
        (
            "EGFR",
            254637,
            2,
            1,
            2,
            5,
            10,
            1.33,
            0.3,
            0.4,
            3.6667,
            1.2993,
            1.2839,
            0.296,
            45.4,
            0.31,
            0.15,
            0.55,
            "TIER_2_CANDIDATE",
            "2023-01-01 00:00:00",
            "run_001",
        ),
        # 2. Defective: Malformed gene symbol "gene!invalid"
        (
            "gene!invalid",
            254637,
            2,
            1,
            2,
            5,
            10,
            1.33,
            0.3,
            0.4,
            3.6667,
            1.2993,
            1.2839,
            0.296,
            45.4,
            0.31,
            0.15,
            0.55,
            "TIER_2_CANDIDATE",
            "2023-01-01 00:00:00",
            "run_001",
        ),
        # 3. Defective: Non-positive disease concept ID (-999)
        (
            "BRCA1",
            -999,
            2,
            1,
            2,
            5,
            10,
            1.33,
            0.3,
            0.4,
            3.6667,
            1.2993,
            1.2839,
            0.296,
            45.4,
            0.31,
            0.15,
            0.55,
            "TIER_2_CANDIDATE",
            "2023-01-01 00:00:00",
            "run_001",
        ),
        # 4. Defective: Negative odds ratio (-2.5)
        (
            "KRAS",
            254637,
            2,
            1,
            2,
            5,
            10,
            1.33,
            0.3,
            0.4,
            -2.5,
            1.2993,
            1.2839,
            0.296,
            45.4,
            0.31,
            0.15,
            0.55,
            "TIER_2_CANDIDATE",
            "2023-01-01 00:00:00",
            "run_001",
        ),
        # 5. Defective: Out of bounds tractability score (1.5 > 1.0)
        (
            "TP53",
            254637,
            2,
            1,
            2,
            5,
            10,
            1.33,
            0.3,
            0.4,
            3.6667,
            1.2993,
            1.2839,
            0.296,
            45.4,
            0.31,
            0.15,
            1.50,
            "TIER_1_VALIDATED",
            "2023-01-01 00:00:00",
            "run_001",
        ),
        # 6. Defective: Out of bounds biomarker correlation (2.5 > 1.0)
        (
            "PIK3CA",
            254637,
            2,
            1,
            2,
            5,
            10,
            1.33,
            0.3,
            0.4,
            3.6667,
            1.2993,
            1.2839,
            0.296,
            45.4,
            0.31,
            2.50,
            0.55,
            "TIER_2_CANDIDATE",
            "2023-01-01 00:00:00",
            "run_001",
        ),
    ]

    schema_test = StructType(
        [
            StructField("target_gene_symbol", StringType(), True),
            StructField("disease_concept_id", LongType(), True),
            StructField("carrier_cases", IntegerType(), True),
            StructField("carrier_controls", IntegerType(), True),
            StructField("non_carrier_cases", IntegerType(), True),
            StructField("non_carrier_controls", IntegerType(), True),
            StructField("total_cohort_size", IntegerType(), True),
            StructField("target_mutation_burden", DoubleType(), True),
            StructField("carrier_frequency", DoubleType(), True),
            StructField("phenotype_prevalence", DoubleType(), True),
            StructField("odds_ratio", DoubleType(), True),
            StructField("log_odds_ratio", DoubleType(), True),
            StructField("se_log_odds_ratio", DoubleType(), True),
            StructField("odds_ratio_ci_lower", DoubleType(), True),
            StructField("odds_ratio_ci_upper", DoubleType(), True),
            StructField("p_value", DoubleType(), True),
            StructField("biomarker_correlation", DoubleType(), True),
            StructField("target_tractability_score", DoubleType(), True),
            StructField("evidence_tier", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("mlflow_run_id", StringType(), True),
        ]
    )

    df_candidate = spark.createDataFrame(test_rows, schema_test)

    temp_dir = tempfile.mkdtemp(prefix="quarantine_target_test_")
    try:
        qw = QuarantineDeltaWriter(spark, base_output_dir=temp_dir)
        df_valid, df_quarantine = validate_and_quarantine_target_records(
            df_mart=df_candidate,
            quarantine_writer=qw,
            mlflow_run_id="test_run_discovery",
        )

        valid_rows = df_valid.collect()
        quarantine_rows = df_quarantine.collect()

        # 1 valid row, 5 quarantined rows
        assert len(valid_rows) == 1
        assert valid_rows[0]["target_gene_symbol"] == "EGFR"

        assert len(quarantine_rows) == 5

        # Verify dead-letter attributes
        for q_row in quarantine_rows:
            assert q_row["table_name"] == QUARANTINE_TABLE_TARGETS
            assert q_row["failure_code"] == ClinicalFailureCode.TARGET_CONTRACT_VIOLATION
            assert q_row["status"] == "QUARANTINED"
            assert q_row["mlflow_run_id"] == "test_run_discovery"

            # Parse raw payload to verify zero data loss
            payload = json.loads(q_row["raw_payload"])
            assert "target_gene_symbol" in payload
            assert payload["target_gene_symbol"] in [
                "gene!invalid",
                "BRCA1",
                "KRAS",
                "TP53",
                "PIK3CA",
            ]

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_delta_persistence_with_liquid_clustering(
    spark: SparkSession, target_discovery_synthetic_data
):
    """Verifies Delta Lake persistence with Liquid Clustering configuration."""
    mart = TargetEvidenceMart(spark)
    df_cohort = target_discovery_synthetic_data["cohort"]
    df_cond = target_discovery_synthetic_data["condition"]
    df_meas = target_discovery_synthetic_data["measurement"]

    df_mart = mart.build_target_evidence_mart(df_cohort, df_cond, df_meas)

    temp_delta_dir = tempfile.mkdtemp(prefix="delta_discovery_mart_test_")
    try:
        saved_path = mart.persist_target_mart(
            df_mart=df_mart,
            base_output_dir=temp_delta_dir,
            mode="overwrite",
        )

        assert os.path.exists(saved_path)
        delta_log_dir = os.path.join(saved_path, "_delta_log")
        assert os.path.exists(delta_log_dir)
        commit_files = [f for f in os.listdir(delta_log_dir) if f.endswith(".json")]
        assert len(commit_files) > 0

        # Read back from Delta with Windows NativeIO fallback
        try:
            df_readback = spark.read.format("delta").load(saved_path)
            assert df_readback.count() == df_mart.count()
            assert "target_gene_symbol" in df_readback.columns
            assert "disease_concept_id" in df_readback.columns
        except (AnalysisException, Py4JJavaError, OSError):
            parquet_files = [
                os.path.join(saved_path, f)
                for f in os.listdir(saved_path)
                if f.endswith(".parquet")
            ]
            assert len(parquet_files) > 0
            row_count = sum(pq.read_table(pf).num_rows for pf in parquet_files)
            assert row_count == df_mart.count()

    finally:
        shutil.rmtree(temp_delta_dir, ignore_errors=True)


def test_extract_target_variant_carriers_empty_cohort(
    spark: SparkSession, target_discovery_synthetic_data
):
    """Verifies that providing an empty cohort restricts results to 0 rows rather than bypassing filter."""
    mart = TargetEvidenceMart(spark)
    df_meas = target_discovery_synthetic_data["measurement"]
    empty_cohort = spark.createDataFrame(
        [],
        StructType([StructField("subject_id", LongType(), False)]),
    )

    df_carriers = mart.extract_target_variant_carriers(df_meas, df_cohort=empty_cohort)
    assert df_carriers.count() == 0
    assert "target_gene_symbol" in df_carriers.columns
    assert "is_carrier" in df_carriers.columns


def test_extract_phenotype_diagnoses_empty_cohort(
    spark: SparkSession, target_discovery_synthetic_data
):
    """Verifies that providing an empty cohort restricts diagnoses to 0 rows rather than bypassing filter."""
    mart = TargetEvidenceMart(spark)
    df_cond = target_discovery_synthetic_data["condition"]
    empty_cohort = spark.createDataFrame(
        [],
        StructType([StructField("person_id", LongType(), False)]),
    )

    df_pheno = mart.extract_phenotype_diagnoses(df_cond, df_cohort=empty_cohort)
    assert df_pheno.count() == 0
    assert "disease_concept_id" in df_pheno.columns
