---
name: omop-cdm-normalizer
description: >-
  Normalize clinical and multi-omics data into OHDSI OMOP CDM v5.4 relational tables.
  Use when ingesting new clinical or genomic datasets, updating domain transformers
  (PERSON, CONDITION_OCCURRENCE, MEASUREMENT), configuring vocabulary mappings,
  or writing Gold-tier Delta tables with Liquid Clustering.
---

# OMOP CDM v5.4 Normalization Engine

Guide for transforming raw/Silver clinical and genomic data into standard OHDSI OMOP CDM v5.4 tables.

## 1. Domain Transformers

All transformers reside in `analytical-layer/omop_cdm_v54/`:

| Domain | Source Module | OMOP CDM Table | Key Transformations |
| :--- | :--- | :--- | :--- |
| Demographics | `person.py` | `PERSON` | Gender, race, ethnicity concept resolution, birth date decomposition (`year`, `month`, `day`). |
| Diagnoses | `condition_occurrence.py` | `CONDITION_OCCURRENCE` | ICD-10-CM to SNOMED CT resolution (handling dotted and dotless codes), EHR primary diagnosis concept 32817. |
| Labs | `measurement.py` | `MEASUREMENT` | LOINC to standard concept resolution, numeric value casting, unit source preservation. |
| Genomics | `genomic_variants.py` | `MEASUREMENT` | VCF INFO field parsing (`CLNSIG`, `GENE`), ClinVar clinical significance mapping, call metric typing. |

## 2. Mandatory OHDSI & GxP Standards

1. **64-bit Signed Primary & Foreign Keys**:
   - Always derive IDs via `xxhash64(...).cast("long")` without `abs()`.
   - Primary key hash inputs must combine deterministic entity keys (e.g. `encounter_id + code + date`).
2. **Dynamic Vocabulary Resolution**:
   - Load concepts using `load_concept_mappings()` from `governance/concept_mappings.json`.
   - Never hardcode concept IDs directly in transformers without a fallback synchronization check.
   - Unmapped source codes must resolve to concept ID `0`.
3. **No Eager PySpark Actions**:
   - Do not call `df.rdd.isEmpty()` or unconditional `df.count()` inside helper functions.
   - Use `df.limit(1).count() == 0` for early return checks.

## 3. Workflow Steps

1. **Bronze Ingestion**:
   - Ingest data via `analytical-layer/omop_cdm_v54/connectors.py` functions (`load_demographics_data`, `load_diagnoses_data`, `load_labs_data`, `load_genomics_data`).
2. **Silver GxP Filtering & Dead-Letter Quarantine**:
   - Apply timestamp parsing (`parsed_birth_dt`, `parsed_diag_dt`, `parsed_lab_dt`) and validation filters.
   - Route rejected rows to `format_quarantine_dataframe()` in `analytical-layer/medallion/quarantine.py`.
3. **Gold Transformation**:
   - Invoke domain transformers to produce OMOP DataFrames.
   - For measurements, union clinical labs and genomic variant measurements using `df_labs.unionByName(df_genomics, allowMissingColumns=True)`.
4. **Delta Lake Persistence with Liquid Clustering**:
   - Use `DeltaMedallionWriter` in `analytical-layer/medallion/writer.py`:
     ```python
     writer = DeltaMedallionWriter(spark, base_output_dir=output_dir)
     writer.write_gold_omop_table(df_person, "person", cluster_by=["person_id"])
     writer.write_gold_omop_table(df_conditions, "condition_occurrence", cluster_by=["person_id", "condition_concept_id"])
     writer.write_gold_omop_table(df_measurements, "measurement", cluster_by=["person_id", "measurement_concept_id"])
     ```

## 4. Verification

Run the pipeline in demo mode and verify the Delta output:
```powershell
.\.venv\Scripts\python.exe analytical-layer/omop_cdm_v54/pipeline.py --mode demo --save_delta
.\.venv\Scripts\pytest.exe tests/unit/test_person.py tests/unit/test_condition_occurrence.py tests/unit/test_measurement.py tests/unit/test_genomic_variants.py -v
```
