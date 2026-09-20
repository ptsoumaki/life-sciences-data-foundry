"""
Unit tests for data ingestion connectors (connectors.py).
Tests resolution of data directories, S3A configuration, VCF parsing,
and Bronze tier demo data loaders.
"""

import os
from unittest.mock import MagicMock

import pytest

from omop_cdm_v54.connectors import (
    configure_s3a_anonymous_access,
    load_demographics_data,
    load_diagnoses_data,
    load_genomics_data,
    load_labs_data,
    parse_vcf_to_dataframe,
    read_http_csv,
    resolve_data_dir,
)


def test_resolve_data_dir_explicit():
    custom_path = "/tmp/custom_data_dir"
    assert resolve_data_dir(custom_path) == custom_path


def test_resolve_data_dir_env_var(monkeypatch, tmp_path):
    monkeypatch.setenv("LSDF_DATA_DIR", str(tmp_path))
    assert resolve_data_dir() == str(tmp_path)


def test_resolve_data_dir_default():
    resolved = resolve_data_dir()
    assert os.path.isabs(resolved)
    assert resolved.endswith("data")


def test_configure_s3a_anonymous_access():
    mock_builder = MagicMock()
    mock_builder.config.return_value = mock_builder

    result = configure_s3a_anonymous_access(mock_builder)
    assert result == mock_builder
    assert mock_builder.config.call_count >= 3


def test_parse_vcf_to_dataframe(spark, tmp_path):
    vcf_content = (
        "##fileformat=VCFv4.2\n"
        "##source=TestSource\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "chr1\t1000\trs123\tA\tG\t99\tPASS\tGENE=BRCA1;CLNSIG=Pathogenic\n"
        "chr2\t2000\trs456\tC\tT\t50\tLowQual\tGENE=TP53;CLNSIG=Benign\n"
    )
    vcf_file = tmp_path / "test_variants.vcf"
    vcf_file.write_text(vcf_content, encoding="utf-8")

    df = parse_vcf_to_dataframe(spark, str(vcf_file))
    assert df.count() == 2
    assert "chrom" in df.columns
    assert "pos" in df.columns
    assert "id" in df.columns
    assert "filter" in df.columns
    assert "ingestion_timestamp" in df.columns

    # Test max_rows limiting
    df_limited = parse_vcf_to_dataframe(spark, str(vcf_file), max_rows=1)
    assert df_limited.count() == 1


def test_parse_vcf_missing_header_raises(spark, tmp_path):
    invalid_vcf = tmp_path / "invalid.vcf"
    invalid_vcf.write_text("##fileformat=VCFv4.2\nchr1\t1000\n", encoding="utf-8")

    with pytest.raises(ValueError, match="No #CHROM header line found"):
        parse_vcf_to_dataframe(spark, str(invalid_vcf))


def test_load_demographics_data_demo(spark):
    df = load_demographics_data(spark, mode="demo")
    assert df.count() > 0
    expected_cols = {
        "raw_patient_id",
        "gender",
        "birth_datetime",
        "race",
        "ethnicity",
        "ingestion_timestamp",
    }
    assert expected_cols.issubset(set(df.columns))


def test_load_diagnoses_data_demo(spark):
    df = load_diagnoses_data(spark, mode="demo")
    assert df.count() > 0
    expected_cols = {
        "encounter_id",
        "raw_patient_id",
        "diagnosis_date",
        "icd10_code",
        "diagnosis_description",
        "ingestion_timestamp",
    }
    assert expected_cols.issubset(set(df.columns))


def test_load_labs_data_demo(spark):
    df = load_labs_data(spark, mode="demo")
    assert df.count() > 0
    expected_cols = {
        "lab_event_id",
        "raw_patient_id",
        "loinc_code",
        "test_name",
        "numeric_value",
        "unit_value",
        "ingestion_timestamp",
    }
    assert expected_cols.issubset(set(df.columns))


def test_load_genomics_data_demo(spark):
    df = load_genomics_data(spark, mode="demo")
    assert df.count() > 0
    expected_cols = {
        "chrom",
        "pos",
        "id",
        "ref",
        "alt",
        "qual",
        "filter",
        "info",
        "ingestion_timestamp",
    }
    assert expected_cols.issubset(set(df.columns))


def test_read_http_csv_fallback(spark, tmp_path):
    fallback_csv = tmp_path / "fallback.csv"
    fallback_csv.write_text("colA,colB\nval1,val2\n", encoding="utf-8")

    df = read_http_csv(
        spark, "http://invalid-non-existent-url.local/data.csv", fallback_path=str(fallback_csv)
    )
    assert df.count() == 1
    assert "colA" in df.columns


def test_read_http_csv_no_fallback_raises(spark):
    with pytest.raises(FileNotFoundError):
        read_http_csv(
            spark,
            "http://invalid-non-existent-url.local/data.csv",
            fallback_path="/non/existent/path.csv",
        )
