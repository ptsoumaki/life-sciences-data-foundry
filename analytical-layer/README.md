# Analytical Data Layer — OMOP CDM v5.4 Normalization Engine 📊

This component implements **Phase 3: Clinical Normalization Ring** — a PySpark Medallion Delta Lake pipeline that transforms raw clinical records and genomic variant metrics into standard [OHDSI OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html) relational tables.

---

## Architecture

The pipeline follows a three-tier **Medallion Architecture** pattern:

```text
┌─────────────────────────────────────────────────────────┐
│ BRONZE TIER: Raw Append-Only Ingestion                  │
│   ├── Raw clinical demographics (patient records)       │
│   └── Nextflow genomic variant quality metrics          │
├─────────────────────────────────────────────────────────┤
│ SILVER TIER: GxP Data Quality Contract Validation       │
│   ├── Timestamp parsing & format validation             │
│   ├── Gender/race/ethnicity field normalization         │
│   └── Failed records → Quarantine Delta dataset         │
├─────────────────────────────────────────────────────────┤
│ GOLD TIER: OHDSI OMOP CDM v5.4 Target Tables           │
│   ├── PERSON table (demographics & concept IDs)         │
│   └── MEASUREMENT table (genomic variant metrics)       │
└─────────────────────────────────────────────────────────┘
```

---

## OMOP CDM v5.4 Target Tables

### PERSON Table

| Column | OMOP Concept | Mapping Logic |
| --- | --- | --- |
| `person_id` | Primary key | `abs(hash(raw_patient_id))` |
| `gender_concept_id` | `8507`=Male, `8532`=Female, `0`=Unknown | Mapped from `raw_gender` |
| `year_of_birth` | Year component | Extracted from `birth_datetime` |
| `birth_datetime` | ISO timestamp | Parsed from raw input |
| `race_concept_id` | `8527`=White, `8515`=Asian, `8516`=Black | Mapped from `raw_race` |
| `ethnicity_concept_id` | `38003564`=Not Hispanic, `38003563`=Hispanic | Mapped from `raw_ethnicity` |

### MEASUREMENT Table (Genomic Variant Metrics)

| Column | OMOP Concept | Mapping Logic |
| --- | --- | --- |
| `measurement_id` | Primary key | `abs(hash(patient+sample+chr+pos))` |
| `person_id` | Foreign key → PERSON | `abs(hash(raw_patient_id))` |
| `measurement_concept_id` | `35917873` | Genomic variant quality assessment |
| `value_as_number` | Numeric result | `quality_score` from Nextflow QC |
| `value_source_value` | Source notation | `chr:pos:ref:alt` format |

---

## Usage

```bash
# Run locally with PySpark
python analytical-layer/omop_mapping.py

# In Databricks, import as notebook and execute against Delta Lake paths
```

---

## Dependencies

- `pyspark >= 3.5.0`
- `delta-spark >= 3.1.0`
