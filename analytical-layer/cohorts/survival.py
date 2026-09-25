"""
Module: survival.py
Description: Time-to-Event (TTE) & Biostatistical Survival Analysis Marts.
             Computes Overall Survival (OS), Time-to-Progression (TTP), and Event-Free Survival (EFS)
             analytical frames from OMOP CDM v5.4 and OHDSI COHORT tables, incorporating clinical covariates,
             administrative/observational right-censoring, multi-omics ClinVar genomic strata,
             and distributed Kaplan-Meier product-limit estimation with Greenwood standard errors.
Author: Vivi Tsoumaki
"""

import logging
import os
from dataclasses import dataclass, field
from enum import StrEnum

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    date_add,
    datediff,
    exp,
    greatest,
    least,
    lit,
    log,
    row_number,
    sqrt,
    to_date,
    upper,
    when,
    year,
)
from pyspark.sql.functions import (
    count as spark_count,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

from omop_cdm_v54.compat import HAS_DELTA

try:
    from medallion.writer import DeltaMedallionWriter
except ImportError:
    DeltaMedallionWriter = None  # type: ignore[assignment, misc]


logger = logging.getLogger(__name__)


class SurvivalEndpoint(StrEnum):
    """Supported biostatistical survival and time-to-event endpoint definitions."""

    OVERALL_SURVIVAL = "OS"
    TIME_TO_PROGRESSION = "TTP"
    EVENT_FREE_SURVIVAL = "EFS"


# Standard Survival Mart Frame Schema
SURVIVAL_FRAME_SCHEMA: StructType = StructType(
    [
        StructField("cohort_definition_id", LongType(), nullable=False),
        StructField("subject_id", LongType(), nullable=False),
        StructField("cohort_start_date", DateType(), nullable=False),
        StructField("time_to_event_days", IntegerType(), nullable=False),
        StructField("event", IntegerType(), nullable=False),
        StructField("censoring_reason", StringType(), nullable=False),
        StructField("age_at_index", IntegerType(), nullable=True),
        StructField("gender_concept_id", LongType(), nullable=True),
        StructField("has_pathogenic_variant", IntegerType(), nullable=False),
        StructField("stratum", StringType(), nullable=False),
    ]
)

# Standard Kaplan-Meier Summary Table Schema
KAPLAN_MEIER_SCHEMA: StructType = StructType(
    [
        StructField("stratum", StringType(), nullable=False),
        StructField("time_to_event_days", IntegerType(), nullable=False),
        StructField("n_at_risk", LongType(), nullable=False),
        StructField("n_events", LongType(), nullable=False),
        StructField("n_censored", LongType(), nullable=False),
        StructField("survival_probability", DoubleType(), nullable=False),
        StructField("standard_error", DoubleType(), nullable=False),
    ]
)


@dataclass
class SurvivalConfig:
    """Configuration parameters for Time-to-Event (TTE) Mart extraction."""

    endpoint: SurvivalEndpoint = SurvivalEndpoint.OVERALL_SURVIVAL
    target_event_concept_ids: list[int] = field(default_factory=list)
    censor_at_death: bool = True
    study_end_date: str | None = None
    max_followup_days: int | None = None
    genomic_concept_id: int = 35917873  # Standard OMOP genomic variant concept ID
    default_censor_window_days: int = 730


class SurvivalMartBuilder:
    """Engine for building Time-to-Event (TTE) analytical marts and Kaplan-Meier tables.

    Translates OHDSI cohorts and OMOP clinical/genomic records into analytical frames
    ready for Cox Proportional Hazards, Kaplan-Meier curves, and ML survival models.
    """

    def __init__(self, spark: SparkSession, config: SurvivalConfig | None = None) -> None:
        """Initializes the survival mart builder.

        Args:
            spark: Active PySpark SparkSession.
            config: Optional SurvivalConfig. Defaults to Overall Survival with standard settings.
        """
        self.spark = spark
        self.config = config or SurvivalConfig()

    def build_survival_frame(
        self,
        df_cohort: DataFrame,
        df_person: DataFrame,
        df_condition_occurrence: DataFrame | None = None,
        df_death: DataFrame | None = None,
        df_observation_period: DataFrame | None = None,
        df_measurement: DataFrame | None = None,
    ) -> DataFrame:
        """Constructs an individual-level Time-to-Event (TTE) analytical frame.

        Args:
            df_cohort: OHDSI COHORT DataFrame (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date).
            df_person: OMOP PERSON DataFrame (person_id, year_of_birth, gender_concept_id).
            df_condition_occurrence: Optional OMOP CONDITION_OCCURRENCE for progression/recurrence events.
            df_death: Optional OMOP DEATH table for overall survival and mortality censoring.
            df_observation_period: Optional OMOP OBSERVATION_PERIOD for administrative censoring.
            df_measurement: Optional OMOP MEASUREMENT table for ClinVar pathogenic variant strata.

        Returns:
            DataFrame conforming to SURVIVAL_FRAME_SCHEMA.
        """
        if df_cohort.limit(1).count() == 0:
            return self.spark.createDataFrame([], SURVIVAL_FRAME_SCHEMA)

        # 1. Base Cohort with Demographics & Age at Index
        df_base = (
            df_cohort.select(
                col("cohort_definition_id").cast(LongType()),
                col("subject_id").cast(LongType()),
                to_date(col("cohort_start_date")).alias("cohort_start_date"),
                to_date(col("cohort_end_date")).alias("cohort_end_date"),
            )
            .join(
                df_person.select(
                    col("person_id").cast(LongType()).alias("subject_id"),
                    col("gender_concept_id").cast(LongType()),
                    col("year_of_birth").cast(IntegerType()),
                ),
                on="subject_id",
                how="left",
            )
            .withColumn(
                "age_at_index",
                (year(col("cohort_start_date")) - col("year_of_birth")).cast(IntegerType()),
            )
        )

        # 2. Extract Valid Death Date (death_date >= cohort_start_date)
        if df_death is not None and df_death.limit(1).count() > 0:
            df_death_clean = (
                df_death.select(
                    col("person_id").cast(LongType()).alias("subject_id"),
                    to_date(col("death_date")).alias("death_date"),
                )
                .filter(col("death_date").isNotNull())
                .distinct()
            )
            df_base = df_base.join(df_death_clean, on="subject_id", how="left")
        else:
            df_base = df_base.withColumn("death_date", lit(None).cast(DateType()))

        # 3. Extract Earliest Progression / Secondary Event Date (if TTP or EFS)
        if (
            self.config.endpoint
            in (SurvivalEndpoint.TIME_TO_PROGRESSION, SurvivalEndpoint.EVENT_FREE_SURVIVAL)
            and df_condition_occurrence is not None
            and df_condition_occurrence.limit(1).count() > 0
            and self.config.target_event_concept_ids
        ):
            w_prog = Window.partitionBy("subject_id").orderBy("condition_start_date")
            prog_events = (
                df_condition_occurrence.filter(
                    col("condition_concept_id").isin(self.config.target_event_concept_ids)
                )
                .select(
                    col("person_id").cast(LongType()).alias("subject_id"),
                    to_date(col("condition_start_date")).alias("condition_start_date"),
                )
                .withColumn("rn", row_number().over(w_prog))
                .filter(col("rn") == 1)
                .select(
                    col("subject_id"),
                    col("condition_start_date").alias("progression_date"),
                )
            )
            df_base = df_base.join(prog_events, on="subject_id", how="left")
        else:
            df_base = df_base.withColumn("progression_date", lit(None).cast(DateType()))

        # 4. Extract Observation Period Right-Censoring Date
        if df_observation_period is not None and df_observation_period.limit(1).count() > 0:
            obs_max = df_observation_period.groupBy(
                col("person_id").cast(LongType()).alias("subject_id")
            ).agg(spark_max(to_date(col("observation_period_end_date"))).alias("obs_period_end"))
            df_base = df_base.join(obs_max, on="subject_id", how="left")
        else:
            df_base = df_base.withColumn("obs_period_end", lit(None).cast(DateType()))

        # 5. Multi-Omics Genomic Stratification (ClinVar Pathogenic Variants)
        if df_measurement is not None and df_measurement.limit(1).count() > 0:
            pathogenic_expr = (col("measurement_concept_id") == self.config.genomic_concept_id) & (
                upper(col("value_source_value")).contains("PATHOGENIC")
                | col("value_as_concept_id").isin([4181412, 36768280])
            )
            genomic_carriers = (
                df_measurement.filter(pathogenic_expr)
                .select(col("person_id").cast(LongType()).alias("subject_id"))
                .distinct()
                .withColumn("has_pathogenic_variant", lit(1))
            )
            df_base = df_base.join(genomic_carriers, on="subject_id", how="left").withColumn(
                "has_pathogenic_variant",
                coalesce(col("has_pathogenic_variant"), lit(0)).cast(IntegerType()),
            )
        else:
            df_base = df_base.withColumn("has_pathogenic_variant", lit(0).cast(IntegerType()))

        # 6. Determine Effective Censor Cutoff Date
        # Precedence: study_end_date > obs_period_end > cohort_end_date > default_censor_window_days
        obs_censor = coalesce(
            col("obs_period_end"),
            col("cohort_end_date"),
        )
        if self.config.study_end_date:
            admin_end = to_date(lit(self.config.study_end_date))
            effective_censor_expr = coalesce(
                least(obs_censor, admin_end),
                admin_end,
                obs_censor,
            )
            # is_study_end_expr is a lazy Column expression; it references
            # "effective_censor_date" which is added to df_base one line below.
            # This is safe because PySpark Column expressions are not evaluated
            # until an action (collect/write) is triggered.
            is_study_end_expr = col("effective_censor_date") == admin_end
        else:
            default_window_censor = date_add(
                col("cohort_start_date"), lit(self.config.default_censor_window_days)
            )
            effective_censor_expr = coalesce(
                obs_censor,
                default_window_censor,
            )
            is_study_end_expr = lit(False)

        df_base = df_base.withColumn("effective_censor_date", effective_censor_expr)

        # 7. Endpoint-Specific Event and Timing Evaluation
        if self.config.endpoint == SurvivalEndpoint.OVERALL_SURVIVAL:
            # Event = Death observed on or before effective censoring date
            has_event_expr = (
                col("death_date").isNotNull()
                & (col("death_date") >= col("cohort_start_date"))
                & (col("death_date") <= col("effective_censor_date"))
            )
            df_eval = (
                df_base.withColumn("event", when(has_event_expr, lit(1)).otherwise(lit(0)))
                .withColumn(
                    "event_or_censor_date",
                    when(has_event_expr, col("death_date")).otherwise(col("effective_censor_date")),
                )
                .withColumn(
                    "censoring_reason",
                    when(has_event_expr, lit("DEATH_EVENT"))
                    .when(is_study_end_expr, lit("STUDY_END"))
                    .otherwise(lit("OBSERVATION_END")),
                )
            )

        elif self.config.endpoint == SurvivalEndpoint.TIME_TO_PROGRESSION:
            # Event = Disease progression/relapse observed on or before death and censoring
            has_prog_event = (
                col("progression_date").isNotNull()
                & (col("progression_date") >= col("cohort_start_date"))
                & (col("progression_date") <= col("effective_censor_date"))
                & (col("death_date").isNull() | (col("progression_date") <= col("death_date")))
            )
            died_before_prog = (
                col("death_date").isNotNull()
                & (col("death_date") >= col("cohort_start_date"))
                & (col("death_date") <= col("effective_censor_date"))
                & (col("progression_date").isNull() | (col("death_date") < col("progression_date")))
            )
            if self.config.censor_at_death:
                # When censor_at_death is enabled, deaths that occur before progression
                # are treated as competing risk censoring events for TTP analysis.
                event_or_censor_expr = (
                    when(has_prog_event, col("progression_date"))
                    .when(died_before_prog, col("death_date"))
                    .otherwise(col("effective_censor_date"))
                )
                censoring_reason_expr = (
                    when(has_prog_event, lit("PROGRESSION_EVENT"))
                    .when(died_before_prog, lit("DEATH_CENSORED"))
                    .when(is_study_end_expr, lit("STUDY_END"))
                    .otherwise(lit("OBSERVATION_END"))
                )
            else:
                # When censor_at_death is disabled, deaths before progression are
                # treated as administrative censoring at the effective censor date.
                event_or_censor_expr = when(has_prog_event, col("progression_date")).otherwise(
                    col("effective_censor_date")
                )
                censoring_reason_expr = (
                    when(has_prog_event, lit("PROGRESSION_EVENT"))
                    .when(is_study_end_expr, lit("STUDY_END"))
                    .otherwise(lit("OBSERVATION_END"))
                )
            df_eval = (
                df_base.withColumn("event", when(has_prog_event, lit(1)).otherwise(lit(0)))
                .withColumn("event_or_censor_date", event_or_censor_expr)
                .withColumn("censoring_reason", censoring_reason_expr)
            )

        else:  # EVENT_FREE_SURVIVAL (EFS: first of progression or death constitutes an event)
            # Use coalesce(least(...), individual_fallbacks) so that a single non-null
            # date is captured even when least() returns null due to the other operand
            # being null (Spark SQL: least(x, null) = null).
            first_event_date = coalesce(
                least(col("progression_date"), col("death_date")),
                col("progression_date"),
                col("death_date"),
            )
            has_efs_event = (
                first_event_date.isNotNull()
                & (first_event_date >= col("cohort_start_date"))
                & (first_event_date <= col("effective_censor_date"))
            )
            df_eval = (
                df_base.withColumn("event", when(has_efs_event, lit(1)).otherwise(lit(0)))
                .withColumn(
                    "event_or_censor_date",
                    when(has_efs_event, first_event_date).otherwise(col("effective_censor_date")),
                )
                .withColumn(
                    "censoring_reason",
                    when(
                        has_efs_event & (first_event_date == col("death_date")),
                        lit("DEATH_EVENT"),
                    )
                    .when(
                        has_efs_event,
                        lit("PROGRESSION_EVENT"),
                    )
                    .when(is_study_end_expr, lit("STUDY_END"))
                    .otherwise(lit("OBSERVATION_END")),
                )
            )

        # 8. Compute Duration in Days & Handle Max Follow-up Horizon
        df_time = df_eval.withColumn(
            "raw_time",
            greatest(
                datediff(col("event_or_censor_date"), col("cohort_start_date")),
                lit(0),
            ),
        )

        if self.config.max_followup_days is not None:
            max_days = self.config.max_followup_days
            df_final_time = (
                df_time.withColumn(
                    "event",
                    when(col("raw_time") > max_days, lit(0)).otherwise(col("event")),
                )
                .withColumn(
                    "censoring_reason",
                    when(col("raw_time") > max_days, lit("MAX_FOLLOWUP")).otherwise(
                        col("censoring_reason")
                    ),
                )
                .withColumn(
                    "time_to_event_days",
                    when(col("raw_time") > max_days, lit(max_days)).otherwise(col("raw_time")),
                )
            )
        else:
            df_final_time = df_time.withColumn("time_to_event_days", col("raw_time"))

        # 9. Assign Stratum Label
        df_stratified = df_final_time.withColumn(
            "stratum",
            when(
                col("has_pathogenic_variant") == 1,
                lit("Pathogenic Variant"),
            ).otherwise(lit("Wild-Type / VUS")),
        )

        # 10. Project Standard Schema
        return df_stratified.select(
            col("cohort_definition_id").cast(LongType()),
            col("subject_id").cast(LongType()),
            col("cohort_start_date").cast(DateType()),
            col("time_to_event_days").cast(IntegerType()),
            col("event").cast(IntegerType()),
            col("censoring_reason").cast(StringType()),
            col("age_at_index").cast(IntegerType()),
            col("gender_concept_id").cast(LongType()),
            col("has_pathogenic_variant").cast(IntegerType()),
            col("stratum").cast(StringType()),
        )

    def compute_kaplan_meier_summary(
        self,
        df_survival: DataFrame,
        strata_col: str | None = "stratum",
    ) -> DataFrame:
        """Computes non-parametric Kaplan-Meier survival curves and Greenwood standard errors.

        Evaluates the product-limit survival estimator S(t) = ∏ [1 - d(t_i) / n(t_i)]
        and Greenwood's formula for SE(S(t)) across distinct time intervals in PySpark.

        Args:
            df_survival: Individual-level DataFrame conforming to SURVIVAL_FRAME_SCHEMA.
            strata_col: Column name to stratify estimates by (e.g. 'stratum' or None for pooled cohort).

        Returns:
            DataFrame conforming to KAPLAN_MEIER_SCHEMA.
        """
        if df_survival.limit(1).count() == 0:
            return self.spark.createDataFrame([], KAPLAN_MEIER_SCHEMA)

        strata_expr: Column = col(strata_col) if strata_col is not None else lit("ALL")

        # Aggregate events and censoring at each discrete time point t per stratum
        df_grouped = (
            df_survival.withColumn("_stratum", strata_expr)
            .groupBy("_stratum", "time_to_event_days")
            .agg(
                spark_sum(when(col("event") == 1, lit(1)).otherwise(lit(0))).alias("n_events"),
                spark_sum(when(col("event") == 0, lit(1)).otherwise(lit(0))).alias("n_censored"),
                spark_count(lit(1)).alias("n_records_at_t"),
            )
        )

        # Window over time within each stratum
        w_stratum = Window.partitionBy("_stratum")
        w_ordered = Window.partitionBy("_stratum").orderBy("time_to_event_days")
        w_preceding = w_ordered.rowsBetween(Window.unboundedPreceding, -1)
        w_cumulative = w_ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

        # 1. Compute total cohort size N per stratum and number at risk n(t)
        df_with_risk = (
            df_grouped.withColumn("total_stratum_n", spark_sum("n_records_at_t").over(w_stratum))
            .withColumn(
                "prior_removed",
                coalesce(spark_sum("n_records_at_t").over(w_preceding), lit(0)),
            )
            .withColumn(
                "n_at_risk",
                greatest(col("total_stratum_n") - col("prior_removed"), lit(0)).cast(LongType()),
            )
        )

        # 2. Compute conditional interval survival probability: p(t) = 1 - d(t) / n(t)
        df_cond = df_with_risk.withColumn(
            "p_t",
            when(
                col("n_at_risk") > 0,
                (col("n_at_risk") - col("n_events")).cast(DoubleType())
                / col("n_at_risk").cast(DoubleType()),
            ).otherwise(lit(0.0)),
        )

        # 3. Product-limit cumulative survival probability S(t) = ∏ p(u)
        # Using logarithmic summation: exp(∑ ln(p(u))) for p(u) > 0
        df_surv = (
            df_cond.withColumn(
                "min_p_to_date",
                spark_min("p_t").over(w_cumulative),
            )
            .withColumn(
                "log_p",
                when(col("p_t") > 0.0, log(col("p_t"))).otherwise(lit(0.0)),
            )
            .withColumn(
                "cum_log_p",
                spark_sum("log_p").over(w_cumulative),
            )
            .withColumn(
                "survival_probability",
                when(col("min_p_to_date") <= 0.0, lit(0.0)).otherwise(
                    spark_round(exp(col("cum_log_p")), 4)
                ),
            )
        )

        # 4. Greenwood's formula: Var(S(t)) = S(t)^2 * ∑ [ d(u) / (n(u) * (n(u) - d(u))) ]
        greenwood_term = when(
            (col("n_at_risk") > col("n_events")) & (col("n_at_risk") > 0) & (col("n_events") > 0),
            col("n_events").cast(DoubleType())
            / (
                col("n_at_risk").cast(DoubleType())
                * (col("n_at_risk") - col("n_events")).cast(DoubleType())
            ),
        ).otherwise(lit(0.0))

        df_greenwood = (
            df_surv.withColumn("greenwood_term", greenwood_term)
            .withColumn("var_sum", spark_sum("greenwood_term").over(w_cumulative))
            .withColumn(
                "standard_error",
                when(
                    col("survival_probability") <= 0.0,
                    lit(0.0),
                ).otherwise(spark_round(col("survival_probability") * sqrt(col("var_sum")), 4)),
            )
        )

        return df_greenwood.select(
            col("_stratum").alias("stratum").cast(StringType()),
            col("time_to_event_days").cast(IntegerType()),
            col("n_at_risk").cast(LongType()),
            col("n_events").cast(LongType()),
            col("n_censored").cast(LongType()),
            col("survival_probability").cast(DoubleType()),
            col("standard_error").cast(DoubleType()),
        ).orderBy("stratum", "time_to_event_days")

    def save_survival_mart(
        self,
        df_survival: DataFrame,
        base_output_dir: str,
        mart_name: str = "survival_mart",
        mode: str = "overwrite",
    ) -> str:
        """Persists the Survival Analytical Mart to Delta Lake or Parquet.

        Args:
            df_survival: Survival DataFrame.
            base_output_dir: Root storage directory.
            mart_name: Subfolder/table name (default 'survival_mart').
            mode: Write mode ('overwrite' or 'append').

        Returns:
            Destination path string.
        """
        target_path = os.path.join(base_output_dir, mart_name)

        if HAS_DELTA and DeltaMedallionWriter is not None:
            try:
                (
                    df_survival.write.format("delta")
                    .mode(mode)
                    .option("mergeSchema", "true")
                    .clusterBy("cohort_definition_id", "subject_id")
                    .save(target_path)
                )
                logger.info(
                    "[SURVIVAL MART] Persisted to Delta Lake with Liquid Clustering: %s",
                    target_path,
                )
                return target_path
            except (AnalysisException, OSError) as lc_err:
                logger.warning(
                    "[SURVIVAL MART] Liquid Clustering write failed; falling back to standard Delta: %s",
                    lc_err,
                )
                df_survival.write.format("delta").mode(mode).save(target_path)
                return target_path
        else:
            df_survival.write.mode(mode).parquet(target_path)
            logger.info("[SURVIVAL MART] Persisted to Parquet: %s", target_path)
            return target_path
