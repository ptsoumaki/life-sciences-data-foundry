# Analytical Data Layer — Modular OMOP CDM v5.4 Normalization Engine 📊

This component implements the **Clinical Normalization & Medallion Engine** — a modular PySpark Medallion Delta Lake pipeline that transforms raw clinical RWE records, lab biomarker observations, ICD-10 diagnoses, and VCF genomic variant calls into standard [OHDSI OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html) relational tables.

---

## Package Architecture

```text
analytical-layer/
├── data/                             # Real-World Clinical & Multi-Omics Ingestion Data
│   ├── clinical_diagnoses.csv        # ICD-10-CM clinical diagnosis events
│   ├── clinical_patients.csv         # Demographics (Synthea / MIMIC-IV format)
│   ├── genomic_variants.vcf          # VCF v4.2 variant annotations (ClinVar / 1000 Genomes)
│   └── lab_measurements.csv          # LOINC lab biomarker observations
├── omop_cdm_v54/                     # MODULAR PYSPARK OMOP CDM v5.4 DOMAIN PACKAGE
│   ├── __init__.py                   # Package exports & versioning
│   ├── condition_occurrence.py       # ICD-10 to SNOMED CT concept transformer
│   ├── connectors.py                 # Open Data ingestion connector (demo vs remote mode)
│   ├── genomic_variants.py           # VCF parser & variant measurement transformer
│   ├── measurement.py                # LOINC lab biomarker transformer
│   ├── person.py                     # Demographics to OMOP PERSON transformer
│   └── pipeline.py                   # Production Medallion pipeline orchestrator
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
│   ├── Clinical patient demographics (clinical_patients.csv)│
│   ├── ICD-10 diagnosis events (clinical_diagnoses.csv)  │
│   ├── LOINC lab observations (lab_measurements.csv)     │
│   └── VCF v4.2 genomic variant calls (genomic_variants.vcf)│
├─────────────────────────────────────────────────────────┤
│ SILVER TIER: GxP Data Quality Contract Validation       │
│   ├── ISO-8601 timestamp parsing & validation           │
│   ├── Demographics & status quarantine filters          │
│   └── Failed records → Quarantine Delta dataset         │
├─────────────────────────────────────────────────────────┤
│ GOLD TIER: OHDSI OMOP CDM v5.4 Relational Tables        │
│   ├── PERSON (demographics & concept IDs)               │
│   ├── CONDITION_OCCURRENCE (SNOMED diagnosis concepts)  │
│   └── MEASUREMENT (LOINC lab panels & genomic variants) │
└─────────────────────────────────────────────────────────┘
```

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

## Execution Modes

The pipeline supports **Dual Ingestion Modes** via Open Data Connectors ([`omop_cdm_v54/connectors.py`](omop_cdm_v54/connectors.py)) and custom dataset directory targeting:

```bash
# Mode A: Execute with local synthetic demo dataset (Default / Instant offline demo)
python analytical-layer/omop_cdm_v54/pipeline.py --mode demo

# Mode B: Stream directly from remote public AWS Open Data S3 & NCBI endpoints
python analytical-layer/omop_cdm_v54/pipeline.py --mode remote

# Mode C: Execute on your own custom real-world dataset directory
python analytical-layer/omop_cdm_v54/pipeline.py --mode demo --data_dir /path/to/my_clinical_data
```
