---
name: gxp-quarantine-remediation
description: >-
  Inspect, evaluate, and remediate quarantined records in dead-letter Delta Lake sinks.
  Use when analyzing clinical data quality breaches, resolving unmapped clinical terminologies
  (ICD-10, LOINC), promoting corrected records to Silver tables, or evaluating batch quarantine rejection thresholds.
---

# GxP Dead-Letter Quarantine & Remediation

Runbook for inspecting, managing, and remediating records isolated in Delta Lake dead-letter sinks.

## 1. Architecture & Table Sinks

Quarantine sinks are managed by `analytical-layer/medallion/quarantine.py`:

- `quarantine_patients`: Schema violations, unparseable birth dates, invalid gender codes.
- `quarantine_conditions`: Unparseable diagnosis dates, missing diagnosis codes.
- `quarantine_measurements`: Unparseable lab dates, out-of-bounds numeric biomarker values, failed genomic quality filters.

All records adhere to `QUARANTINE_RECORD_SCHEMA`:
`[quarantine_id, table_name, raw_payload, failure_code, failure_reason, failure_timestamp, mlflow_run_id, status, remediation_timestamp]`

## 2. Standard Failure Codes

Defined in `ClinicalFailureCode(StrEnum)`:
- `SCHEMA_VIOLATION`: Missing required keys or corrupted columns.
- `UNMAPPED_TERMINOLOGY`: Code cannot be mapped to a standard OMOP concept.
- `OUT_OF_BOUNDS_LAB`: Measurement outside plausible biological ranges.
- `TEMPORAL_ANOMALY`: Chronological order violation (e.g. event precedes birth).
- `ORPHAN_FOREIGN_KEY`: Foreign key references a non-existent parent entity.

## 3. Remediation Procedure

When unmapped codes are identified:

1. **Inspect Unresolved Records**:
   ```python
   from medallion.quarantine import QuarantineRemediationEngine

   engine = QuarantineRemediationEngine(spark, base_output_dir="data/delta_warehouse")
   df_unresolved = engine.get_unresolved_records("quarantine_conditions")
   df_unresolved.select("quarantine_id", "failure_code", "failure_reason", "raw_payload").show(
       truncate=False
   )
   ```
2. **Update Vocabulary Specifications**:
   - Add new code cross-references to `governance/concept_mappings.json`.
   - Ensure corresponding fallback dict in `analytical-layer/omop_cdm_v54/vocabularies.py` is updated in sync.
3. **Execute Remediation & Promotion**:
   - Call remediation engine with updated mappings:
     ```python
     result = engine.remediate_conditions(updated_concept_mappings={"NEW_CODE": 123456})
     # Or for labs:
     # result = engine.remediate_measurements(updated_loinc_mappings={"9999-9": 654321})
     print(f"Remediated: {result['remediated_count']} / {result['total_evaluated']}")
     ```
   - Remediated records are automatically:
     - Promoted to Silver tables via idempotent Delta MERGE (`upsert_silver_table`).
     - Marked with `status = "REMEDIATED"` and `remediation_timestamp` in the quarantine sink.

## 4. GxP Breach Enforceability

Batch quarantine ratios are gated by `evaluate_batch_quarantine_threshold()`:
- Default threshold: `0.02` (2.0% tolerable rejection ratio).
- When `abort_on_breach=True`, a rejection ratio exceeding the threshold raises `GxPBreachError`.
- All metrics are logged to MLflow for 21 CFR Part 11 auditing.

## 5. Verification

Run unit tests covering quarantine capture and remediation:
```powershell
.\.venv\Scripts\pytest.exe tests/unit/test_quarantine.py -v
```
