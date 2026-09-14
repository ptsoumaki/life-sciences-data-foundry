# Analytical Data Layer — Modular OMOP CDM v5.4 Normalization Engine 📊

This component implements the **Clinical Normalization & Medallion Engine** — a modular PySpark Medallion Delta Lake pipeline that transforms raw clinical RWE records, lab biomarker observations, ICD-10 diagnoses, and VCF genomic variant calls into standard [OHDSI OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html) relational tables.

---

## Package Architecture

```text
analytical-layer/
├── cohorts/                          # GOLD-TIER ANALYTICAL COHORTS & TRANSLATIONAL ENDPOINTS
│   ├── __init__.py                   # Package exports & schemas
│   ├── builder.py                    # OHDSI phenotyping engine & cohort builder (T0 index, rules)
│   ├── deid.py                       # HIPAA Safe Harbor de-identification transformer (date shifting, age capping)
│   ├── features.py                   # Patient feature store projections (CCI, comorbidity counts, biomarkers)
│   └── survival.py                   # Time-to-Event marts (OS, TTP, EFS, Kaplan-Meier estimation)
├── data/                             # Real-World Clinical & Multi-Omics Ingestion Data
│   ├── clinical_diagnoses.csv        # ICD-10-CM clinical diagnosis events
│   ├── clinical_patients.csv         # Demographics (Synthea / MIMIC-IV format)
│   ├── genomic_variants.vcf          # VCF v4.2 variant annotations (ClinVar / 1000 Genomes)
│   └── lab_measurements.csv          # LOINC lab biomarker observations
├── medallion/                        # DELTA LAKE PERFORMANCE, STORAGE & GXP QUARANTINE
│   ├── __init__.py                   # Package exports
│   ├── quarantine.py                 # QuarantineRemediationEngine & dead-letter replay
│   └── writer.py                     # DeltaMedallionWriter, quarantine sinks & schema evolution
├── omop_cdm_v54/                     # MODULAR PYSPARK OMOP CDM v5.4 DOMAIN PACKAGE
│   ├── __init__.py                   # Package exports & versioning
│   ├── compat.py                     # Delta Lake & PySpark runtime compatibility layer
│   ├── condition_occurrence.py       # ICD-10 to SNOMED CT concept transformer
│   ├── connectors.py                 # Open Data ingestion connector (demo vs remote mode)
│   ├── genomic_variants.py           # VCF parser & variant measurement transformer
│   ├── measurement.py                # LOINC lab biomarker transformer
│   ├── person.py                     # Demographics to OMOP PERSON transformer
│   ├── pipeline.py                   # Production Medallion pipeline orchestrator
│   └── vocabularies.py               # Dynamic concept mapping & PySpark lookup engine
└── README.md                         # Architecture specification & usage
```

---

## 🧬 Data Provenance & Open Public Dataset References

The ingested datasets located under [`analytical-layer/data/`](data/) are structured according to open-access clinical research standards, medical vocabularies, and genomic repositories:

| Dataset File | Domain / Format | Standard Vocabulary & Public Reference | Open Public Dataset Links |
| :--- | :--- | :--- | :--- |
| **`clinical_patients.csv`** | Clinical Demographics | Synthetic cohort modeled on **Synthea™ OMOP CDM** and **PhysioNet MIMIC-IV** demographic schema formats | 🔗 [Synthea Open Health Data](https://synthetichealth.github.io/synthea/)<br>🔗 [OHDSI ETL-Synthea](https://github.com/OHDSI/ETL-Synthea)<br>🔗 [PhysioNet MIMIC-IV Database](https://physionet.org/content/mimiciv/) |
| **`clinical_diagnoses.csv`** | Clinical Diagnoses | Encoded using **ICD-10-CM** (Clinical Modification) and mapped to **SNOMED CT®** standard concept IDs via OHDSI Athena | 🔗 [CDC ICD-10-CM Browser](https://www.cdc.gov/nchs/icd/icd10cm.htm)<br>🔗 [SNOMED International](https://www.snomed.org/)<br>🔗 [OHDSI Athena Vocabularies](https://athena.ohdsi.org/) |
| **`lab_measurements.csv`** | Laboratory Biomarkers | Observation panels mapped to **LOINC®** (Logical Observation Identifiers Names and Codes) standard codes | 🔗 [Regenstrief LOINC Database](https://loinc.org/)<br>🔗 [NIH NLM UMLS Metathesaurus](https://www.nlm.nih.gov/research/umls/index.html) |
| **`genomic_variants.vcf`** | Genomic Variant Calls | **VCF v4.2** (Variant Call Format) annotated GRCh38 genomic coordinates featuring ClinVar pathogenic mutations | 🔗 [NCBI ClinVar Database](https://www.ncbi.nlm.nih.gov/clinvar/)<br>🔗 [1000 Genomes Project](https://www.internationalgenome.org/)<br>🔗 [GA4GH VCF v4.2 Specification](https://samtools.github.io/hts-specs/VCFv4.2.pdf) |

---

## Medallion Data Flow

```text
┌─────────────────────────────────────────────────────────┐
│ BRONZE TIER: Real-World Ingestion Data Streams          │
│   ├── Clinical patient demographics (clinical_patients) │
│   ├── ICD-10 diagnosis events (clinical_diagnoses.csv)  │
│   ├── LOINC lab observations (lab_measurements.csv)     │
│   └── VCF v4.2 genomic variant calls (genomic_variants) │
├─────────────────────────────────────────────────────────┤
│ SILVER TIER: GxP Data Quality & Quarantine Validation   │
│   ├── ISO-8601 timestamp parsing & validation           │
│   ├── Quality contract filtering & batch breach gates   │
│   └── Non-compliant records → Quarantine Delta sinks    │
├─────────────────────────────────────────────────────────┤
│ GOLD TIER: OHDSI OMOP CDM v5.4 Relational Tables        │
│   ├── PERSON (demographics & concept IDs)               │
│   ├── CONDITION_OCCURRENCE (SNOMED diagnosis concepts)  │
│   └── MEASUREMENT (LOINC lab panels & genomic variants) │
│   └── Delta Lake Liquid Clustering: CLUSTER BY          │
│       (person_id, concept_id)                           │
└─────────────────────────────────────────────────────────┘
```

---

## ⚡ Delta Lake Storage & GxP Quarantine Engine (`medallion/`)

The [`analytical-layer/medallion/`](medallion/) package provides storage optimization and GxP regulatory resilience across Medallion tiers:

1. **Liquid Clustering (`CLUSTER BY`)**: Replaces static Hive partitioning with dynamic multi-dimensional clustering (`person_id`, `concept_id`), optimizing predicate pushdowns without file fragmentation.
2. **Schema Evolution Contracts (`mergeSchema=True`)**: Enforces controlled schema evolution across Silver and Gold write streams for evolving clinical attributes and variant metadata.
3. **Idempotent MERGE / Upserts (`DeltaTable.merge()`)**: Atomic SCD Type 1 upserts on primary clinical keys (`person_id`, `condition_occurrence_id`, `measurement_id`) to prevent duplicate records during batch replays.
4. **Dead-Letter Quarantine Sinks**: Isolates non-compliant records into dedicated Delta sinks (`quarantine_patients`, `conditions`, `measurements`), preserving raw JSON payloads (ALCOA+ audit trail) categorized by standardized `ClinicalFailureCode` taxonomies.
5. **Batch Breach Gates & Remediation**: Halts pipeline execution with `GxPBreachError` on batch rejection threshold breaches, while `QuarantineRemediationEngine` supports automated replay and promotion into Silver upon terminology updates.
6. **GxP Storage Telemetry**: Exposes `get_table_telemetry()` for Delta transaction history, Change Data Feed (CDF), byte sizes, and clustering layout audits.

---

## OMOP CDM v5.4 Target Tables & Concept Mappings

### 1. PERSON Table
* **Gender**: `8507` = Male, `8532` = Female, `0` = Unknown
* **Race**: `8527` = White, `8515` = Asian, `8516` = Black
* **Ethnicity**: `38003563` = Hispanic, `38003564` = Not Hispanic

### 2. CONDITION_OCCURRENCE Table (SNOMED CT Diagnoses)
* **`E11.9`** (Type 2 Diabetes Mellitus) ➔ **SNOMED `201826`**
* **`I10`** (Essential Primary Hypertension) ➔ **SNOMED `316866`**
* **`J45.909`** (Unspecified Asthma) ➔ **SNOMED `195080`**
* **`I21.9`** (Acute Myocardial Infarction) ➔ **SNOMED `4329847`**
* **`C34.90`** (Malignant Neoplasm of Bronchus/Lung) ➔ **SNOMED `254637`**

### 3. MEASUREMENT Table (LOINC Labs & Genomic Variants)
* **LOINC `4548-4`** (HbA1c Blood Panel) ➔ OMOP Concept `3004410`
* **LOINC `2345-7`** (Serum Glucose) ➔ OMOP Concept `3000483`
* **LOINC `2093-3`** (Total Serum Cholesterol) ➔ OMOP Concept `3004249`
* **LOINC `2160-0`** (Serum Creatinine) ➔ OMOP Concept `3016723`
* **Genomic Variant Quality Assessment** ➔ OMOP Concept `35917873` (VCF QUAL, `ref:alt`, rsID, ClinVar significance)

---

## 📊 Gold-Tier Analytical Cohorts & Translational Endpoints (`cohorts/`)

The [`analytical-layer/cohorts/`](cohorts/) package provides an enterprise, GxP-compliant clinical and multi-omics phenotyping engine:

1. **Declarative OHDSI Phenotyping Engine (`builder.py`)**:
   - `OHDSICohortBuilder`: Evaluates index condition / biomarker events ($T_0$), continuous prior observation lookback windows, demographic filters (age, gender), clinical exclusion conditions, biomarker cutoffs, and ClinVar multi-omics criteria.
   - Outputs standard OHDSI `COHORT` tables (`cohort_definition_id`, `subject_id`, `cohort_start_date`, `cohort_end_date`) persisted with Delta Lake Liquid Clustering (`CLUSTER BY (cohort_definition_id, subject_id)`).
   - Preconfigured reference definitions: `get_type_2_diabetes_cohort_definition()`, `get_hypertension_cohort_definition()`, `get_genomic_oncology_cohort_definition()`.

2. **HIPAA Safe Harbor De-Identification Transformer (`deid.py`)**:
   - Implements strict 45 CFR §164.514(b)(2) Safe Harbor standards:
     - **Deterministic Keyed Pseudonymization**: Maps patient identifiers using salt-seeded 64-bit hashing (`xxhash64`).
     - **Interval-Preserving Date Shifting**: Computes patient-specific $\pm \Delta$ days date shift, uniformly shifting all longitudinal dates while strictly preserving inter-event intervals, treatment durations, and survival follow-up times.
     - **Age Capping (89+)**: Identifies individuals aged $\ge 90$ at index and caps age attributes to 89, nullifying birth datetimes.
     - **Geographic Truncation**: Truncates ZIP codes to 3 digits (ZIP3) with automatic zeroing of low-population prefixes (<20,000 residents).

3. **Time-to-Event (TTE) & Survival Analysis Marts (`survival.py`)**:
   - `SurvivalMartBuilder`: Computes biostatistical analytical frames for **Overall Survival (OS)**, **Time-to-Progression (TTP)**, and **Event-Free Survival (EFS)** with administrative (`study_end_date`), observational, and max follow-up right-censoring.
   - Incorporates clinical covariates (age at index, gender) and multi-omics ClinVar genomic strata (`has_pathogenic_variant`).
   - `compute_kaplan_meier_summary()`: Generates non-parametric Kaplan-Meier survival curves, at-risk numbers, event counts, and Greenwood standard errors directly in distributed PySpark.

4. **ML-Ready Patient Feature Store Projections (`features.py`)**:
   - `PatientFeatureStore`: Compiles wide, numerical, scikit-learn / XGBoost-ready matrices anchored around index date $T_0$.
   - **Charlson Comorbidity Index (CCI)**: Calculates standardized clinical comorbidity risk with Deyo/Quan hierarchical rule exclusions (complicated diabetes, severe liver, metastatic tumor).
   - **Rolling Lookback Windows**: Multi-window condition counts (30d, 180d, 365d, lifetime).
   - **Baseline Biomarker Panels**: Latest observations, 365-day mean/min/max, and explicit missingness indicators for HbA1c, glucose, cholesterol, and creatinine.
   - **Genomic Embeddings**: Binary and count ClinVar pathogenic mutation features.

---

## Execution Modes

The pipeline supports **Dual Ingestion Modes** via Open Data Connectors ([`omop_cdm_v54/connectors.py`](omop_cdm_v54/connectors.py)), cohort generation, and custom dataset directory targeting:

```bash
# Mode A: Execute with local synthetic demo dataset (Default / Instant offline demo)
python analytical-layer/omop_cdm_v54/pipeline.py --mode demo

# Mode B: Execute with full Gold Cohort, Survival Mart, and Feature Store generation
python analytical-layer/omop_cdm_v54/pipeline.py --mode demo --build_cohorts

# Mode C: Stream directly from remote public AWS Open Data S3 & NCBI endpoints
python analytical-layer/omop_cdm_v54/pipeline.py --mode remote

# Mode D: Execute on your own custom real-world dataset directory
python analytical-layer/omop_cdm_v54/pipeline.py --mode demo --data_dir /path/to/my_clinical_data
```
