---
name: cohort-phenotyping-survival
description: >-
  Define clinical study cohorts, apply HIPAA Safe Harbor de-identification, run longitudinal survival analysis, and extract multi-omics feature matrices.
  Use when building patient cohorts, de-identifying data for external research, computing Kaplan-Meier survival curves, or calculating Charlson Comorbidity Indices.
---

# Clinical Cohorts, HIPAA De-Identification & Survival Analysis

Guide for building study cohorts, applying privacy protections, and running statistical analysis.

## 1. Subsystem Architecture

All modules reside in `analytical-layer/cohorts/`:

| Module | Core Classes / Functions | Purpose |
| :--- | :--- | :--- |
| `builder.py` | `CohortDefinition`, `CohortBuilder` | Clinical phenotyping via inclusion/exclusion rules on conditions, labs, and genomics. |
| `deid.py` | `CohortDeidentifier` | HIPAA Safe Harbor de-identification (HMAC-SHA256, date-shifting, age capping, ZIP3). |
| `survival.py` | `SurvivalAnalysisEngine`, `KaplanMeierEstimator` | Longitudinal survival endpoints (OS, TTP, EFS), Kaplan-Meier curves, Greenwood SE. |
| `features.py` | `FeatureStore` | Patient-level feature matrices, Charlson Comorbidity Index, rolling counts, biomarker stats. |

## 2. HIPAA Safe Harbor De-Identification Rules

Always apply `CohortDeidentifier` before sharing or exporting cohort datasets:

1. **Pseudonymization**: Replaces `person_id` / `subject_id` with HMAC-SHA256 hash using `LSDF_DEID_SALT`.
2. **Date-Shifting**: Shifts all patient event dates by a deterministic, patient-specific day offset (retaining exact longitudinal intervals between events).
3. **Age Capping**: Individuals aged ≥ 90 have their year of birth capped to `reference_year - 89`.
4. **Timestamp Clearance**: Detailed `birth_datetime` timestamps are cleared (`NULL`).
5. **Geographic Masking**: Postal codes truncated to 3 digits (ZIP3), with restricted population prefixes (<20,000 residents: 036, 692, 878, etc.) replaced with `"000"`.

## 3. Workflow Example

```python
from cohorts.builder import CohortBuilder, CohortDefinition
from cohorts.deid import CohortDeidentifier
from cohorts.survival import SurvivalAnalysisEngine
from cohorts.features import FeatureStore

# 1. Define and build cohort
cohort_def = CohortDefinition(
    cohort_name="T2D_High_HbA1c",
    target_condition_concepts=[201826],  # Type 2 Diabetes
    min_age=18,
    max_age=85,
    biomarker_concept_id=3004410,  # HbA1c
    biomarker_min_value=8.0,
)
builder = CohortBuilder(spark)
df_cohort = builder.build_cohort(df_person, df_condition, df_measurement, cohort_def)

# 2. De-identify per HIPAA Safe Harbor
deid = CohortDeidentifier(salt="clinical_data_foundry_enterprise_salt_2026")
df_deid_cohort = deid.deidentify_cohort(df_cohort)

# 3. Longitudinal Survival Analysis
survival_engine = SurvivalAnalysisEngine(spark)
df_survival = survival_engine.build_survival_frame(
    df_cohort=df_cohort,
    df_condition_occurrence=df_condition,
    df_death=df_death,
    event_condition_concepts=[4329847],  # Myocardial Infarction
)
km_results = survival_engine.compute_kaplan_meier(df_survival)

# 4. Feature Matrix with Charlson Comorbidity Index
feature_store = FeatureStore(spark)
df_features = feature_store.build_feature_matrix(
    df_cohort=df_cohort,
    df_condition_occurrence=df_condition,
    df_measurement=df_measurement,
)
```

## 4. Verification

Run the cohort test suite:
```powershell
.\.venv\Scripts\pytest.exe tests/unit/test_cohort_builder.py tests/unit/test_deid.py tests/unit/test_survival.py tests/unit/test_features.py -v
```
