"""
Module: deid.py
Description: HIPAA Safe Harbor De-Identification Transformer (45 CFR §164.514(b)(2)).
             Applies deterministic keyed pseudonymization, patient-specific date shifting (±Δ days)
             preserving longitudinal event intervals and survival durations, age 89+ capping,
             and geographic ZIP3 truncation.
Author: Vivi Tsoumaki
"""

import os
import warnings

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    date_add,
    lit,
    substring,
    to_date,
    when,
    xxhash64,
)
from pyspark.sql.types import IntegerType

# Restricted ZIP3 prefixes with population < 20,000 per HHS Safe Harbor rules
RESTRICTED_ZIP3_PREFIXES = frozenset(
    {
        "036",
        "059",
        "102",
        "203",
        "556",
        "692",
        "705",
        "772",
        "821",
        "823",
        "878",
        "879",
        "884",
        "893",
        "909",
    }
)


class HIPAADeIdentifier:
    """HIPAA Privacy Rule Safe Harbor De-Identification Transformer.

    Implements:
    1. Keyed deterministic pseudonymization of patient identifiers.
    2. Salt-seeded patient-specific date shifting (±Δ days) that preserves exact
       inter-event duration, longitudinal intervals, and survival metrics.
    3. Age capping for individuals aged 90 or older (capped at 89).
    4. Geographic masking truncating ZIP codes to ZIP3.
    """

    def __init__(
        self,
        salt: str | None = None,
        max_shift_days: int = 365,
    ):
        """Initializes the de-identifier.

        Args:
            salt: Secret cryptographic seed string. Defaults to LSDF_DEID_SALT
                  env var or a deterministic fallback for demo/development.
            max_shift_days: Maximum bound for uniform date shifting (default ±365 days).
        """
        resolved_salt = salt or os.environ.get("LSDF_DEID_SALT")
        if resolved_salt is None:
            # GxP / HIPAA NOTICE: No salt was provided and LSDF_DEID_SALT is unset.
            # Falling back to a publicly-visible demo seed. This MUST NOT be used in
            # production — set the LSDF_DEID_SALT environment variable to a secret,
            # randomly-generated value before processing real patient data.
            warnings.warn(
                "HIPAADeIdentifier: LSDF_DEID_SALT environment variable is not set. "
                "Using an insecure demo salt. Set LSDF_DEID_SALT before processing "
                "real patient data.",
                stacklevel=2,
            )
            resolved_salt = "LSDF_GxP_SALT_2026_DEFAULT"
        self.salt = resolved_salt
        self.max_shift_days = max(1, max_shift_days)

    def _get_patient_shift_col(self, id_col: str) -> Column:
        """Derives a deterministic patient-specific date shift column in [-max_shift, +max_shift]."""
        range_span = 2 * self.max_shift_days + 1
        raw_hash = xxhash64(concat(col(id_col).cast("string"), lit(f"{self.salt}_SHIFT")))
        pos_mod = ((raw_hash % lit(range_span)) + lit(range_span)) % lit(range_span)
        return (pos_mod - lit(self.max_shift_days)).cast("int")

    def _get_pseudonymized_id_col(self, id_col: str) -> Column:
        """Derives a deterministic pseudonymous 64-bit integer identifier."""
        return xxhash64(concat(col(id_col).cast("string"), lit(f"{self.salt}_PSEUDO_ID"))).cast(
            "long"
        )

    def deidentify_cohort(self, df_cohort: DataFrame) -> DataFrame:
        """Applies HIPAA Safe Harbor de-identification to an OHDSI COHORT table.

        Pseudonymizes subject_id and shifts cohort_start_date and cohort_end_date
        by the exact same patient-specific delta, preserving cohort duration.

        Args:
            df_cohort: DataFrame conforming to COHORT_SCHEMA.

        Returns:
            De-identified DataFrame with shifted dates and pseudonymous subject_ids.
        """
        if df_cohort.limit(1).count() == 0:
            return df_cohort

        shift_col = self._get_patient_shift_col("subject_id")
        pseudo_id_col = self._get_pseudonymized_id_col("subject_id")

        df_deid = (
            df_cohort.withColumn("_shift", shift_col)
            .withColumn(
                "cohort_start_date", date_add(to_date(col("cohort_start_date")), col("_shift"))
            )
            .withColumn("cohort_end_date", date_add(to_date(col("cohort_end_date")), col("_shift")))
            .withColumn("subject_id", pseudo_id_col)
            .drop("_shift")
        )
        return df_deid

    def deidentify_person(
        self,
        df_person: DataFrame,
        reference_year: int = 2026,
    ) -> DataFrame:
        """De-identifies the OMOP CDM PERSON table per HIPAA Safe Harbor rules.

        1. Replaces person_id with pseudonymous surrogate key.
        2. Caps age at 89 for individuals aged >= 90 (recalculates year_of_birth to reference_year - 89).
        3. Clears specific birth_datetime timestamp (only keeping shifted or capped year).
        4. Truncates ZIP/location codes to 3 digits (ZIP3) if present.

        Args:
            df_person: Gold-tier PERSON DataFrame.
            reference_year: Reference calendar year for age calculation (default 2026).

        Returns:
            De-identified PERSON DataFrame.
        """
        if df_person.limit(1).count() == 0:
            return df_person

        pseudo_id_col = self._get_pseudonymized_id_col("person_id")

        df_deid = df_person.withColumn("raw_age", lit(reference_year) - col("year_of_birth"))

        # Age capping rule: if age >= 90, cap year_of_birth to reference_year - 89
        df_deid = df_deid.withColumn(
            "year_of_birth",
            when(col("raw_age") >= 90, lit(reference_year - 89)).otherwise(col("year_of_birth")),
        )

        # HIPAA Safe Harbor (45 CFR §164.514(b)(2)(i)(C)):
        # For individuals aged >= 90, all elements of dates (except year) must be removed.
        if "month_of_birth" in df_deid.columns:
            df_deid = df_deid.withColumn(
                "month_of_birth",
                when(col("raw_age") >= 90, lit(None).cast(IntegerType())).otherwise(
                    col("month_of_birth")
                ),
            )
        if "day_of_birth" in df_deid.columns:
            df_deid = df_deid.withColumn(
                "day_of_birth",
                when(col("raw_age") >= 90, lit(None).cast(IntegerType())).otherwise(
                    col("day_of_birth")
                ),
            )

        # Truncate or remove birth_datetime for HIPAA compliance.
        # Cast the null to the column's original declared type to preserve the Delta Lake
        # schema contract (birth_datetime is commonly TimestampType, not StringType).
        if "birth_datetime" in df_deid.columns:
            original_dt_type = df_deid.schema["birth_datetime"].dataType
            df_deid = df_deid.withColumn("birth_datetime", lit(None).cast(original_dt_type))

        # Mask postal/ZIP codes if present (HIPAA Safe Harbor: 3-digit ZIP truncation & restricted prefix suppression)
        for zip_col_name in ("zip", "zip_code", "postal_code"):
            if zip_col_name in df_deid.columns:
                zip3_col = substring(col(zip_col_name).cast("string"), 1, 3)
                df_deid = df_deid.withColumn(
                    zip_col_name,
                    when(zip3_col.isin(list(RESTRICTED_ZIP3_PREFIXES)), lit("000")).otherwise(
                        zip3_col
                    ),
                )

        df_deid = df_deid.withColumn("person_id", pseudo_id_col).drop("raw_age")
        return df_deid

    def deidentify_longitudinal_table(
        self,
        df_table: DataFrame,
        person_id_col: str,
        date_cols: list[str],
    ) -> DataFrame:
        """Applies consistent patient-specific date shifting and pseudonymization to a longitudinal table.

        Args:
            df_table: Input DataFrame (e.g. CONDITION_OCCURRENCE or MEASUREMENT).
            person_id_col: Column name identifying the patient.
            date_cols: List of date/timestamp columns to shift.

        Returns:
            De-identified DataFrame with shifted dates and pseudonymous person IDs.
        """
        if df_table.limit(1).count() == 0:
            return df_table

        shift_col = self._get_patient_shift_col(person_id_col)
        pseudo_id_col = self._get_pseudonymized_id_col(person_id_col)

        df_deid = df_table.withColumn("_shift", shift_col)
        for d_col in date_cols:
            if d_col in df_deid.columns:
                df_deid = df_deid.withColumn(d_col, date_add(to_date(col(d_col)), col("_shift")))

        df_deid = df_deid.withColumn(person_id_col, pseudo_id_col).drop("_shift")
        return df_deid
