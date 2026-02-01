# OpenMRS to OMOP CDM ETL Pipeline

A healthcare data engineering project transforming Electronic Health Record (EHR) data from OpenMRS into the OMOP Common Data Model (CDM) v5.4 for standardized clinical analytics and research.

## My Contributions

I extended this ETL pipeline by developing **3 new transformation models** that expanded the data coverage by 25%:

### 1. Drug Exposure Model (`drug_exposure.sql`)
Transforms medication records from OpenMRS observations into OMOP's standardized drug exposure format.

**Technical Implementation:**
- 5-table JOIN across `obs`, `encounter`, `concept_name`, `concept_numeric`, and `users`
- Domain-based routing using `concept_mapping` where `domainId = 'Drug'`
- COALESCE for null date handling
- Maps to RxNorm standard drug concepts

```sql
-- Key transformation logic
SELECT o.obs_id AS drug_exposure_id,
       concept_mapping.conceptId AS drug_concept_id,
       ...
FROM openmrs.obs AS o
    INNER JOIN openmrs.encounter e ON o.encounter_id = e.encounter_id
    LEFT JOIN raw.CONCEPT_MAPPING concept_mapping ON o.concept_id = concept_mapping.sourceCode
WHERE concept_mapping.domainId = 'Drug';
```

### 2. Procedure Occurrence Model (`procedure_occurrence.sql`)
Extracts clinical procedures from observations and maps them to OMOP procedure concepts.

**Technical Implementation:**
- Domain filtering for `Procedure` type concepts
- Quantity extraction from numeric observation values
- Provider linkage through user table joins

### 3. Visit Detail Model (`visit_detail.sql`)
Creates granular encounter-level records from OpenMRS encounters, linked to parent visits.

**Technical Implementation:**
- **LAG window function** for temporal sequencing of visits
- Encounter type mapping for visit classification
- Care site and provider attribution

```sql
-- Window function for visit sequencing
LAG(e.encounter_id) OVER (
    PARTITION BY e.patient_id
    ORDER BY e.encounter_datetime
) AS preceding_visit_detail_id
```

## Pipeline Results

| Metric | Count |
|--------|-------|
| Drug Exposure Records | 24 |
| Procedure Records | 3 |
| Visit Detail Records | 290 |
| Total New Records | 317 |

## Architecture

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  OpenMRS MySQL  │      │    SQLMesh      │      │   OMOP CDM      │
│    (Source)     │ ───> │ Transformations │ ───> │  PostgreSQL     │
│                 │      │                 │      │                 │
│  - obs          │      │  15 models:     │      │  - person       │
│  - encounter    │      │  - person       │      │  - visit        │
│  - patient      │      │  - measurement  │      │  - drug_exposure│
│  - concept      │      │  - drug_exposure│      │  - procedure    │
│  - visit        │      │  - procedure    │      │  - visit_detail │
└─────────────────┘      │  - visit_detail │      └─────────────────┘
                         └─────────────────┘
                                 │
                                 v
                      ┌─────────────────────┐
                      │  Data Quality       │
                      │  - Achilles         │
                      │  - DQD Dashboard    │
                      └─────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Source Database | MySQL 8.x |
| Target Database | PostgreSQL 15 |
| ETL Framework | SQLMesh |
| Migration | pgloader |
| Containerization | Docker |
| Data Quality | OHDSI Achilles, DQD |
| Healthcare Standard | OMOP CDM v5.4 |
| Vocabularies | SNOMED CT, RxNorm, LOINC, ICD-10 |

## SQL Techniques Used

- **Multi-table JOINs** (5+ tables)
- **Window Functions** (LAG for temporal sequencing)
- **COALESCE** for null handling
- **Domain-based routing** (Drug, Procedure, Measurement)
- **Concept mapping** (OpenMRS CIEL → OMOP standard concepts)

## Quick Start

```bash
# Start containers
docker compose up -d

# Run full ETL pipeline
docker compose run --rm core run-full-pipeline

# Run data quality analysis
docker compose run --rm achilles
```

## Project Structure

```
├── core/
│   ├── models/
│   │   ├── drug_exposure.sql      # My contribution
│   │   ├── procedure_occurrence.sql   # My contribution
│   │   ├── visit_detail.sql       # My contribution
│   │   ├── person.sql
│   │   ├── measurement.sql
│   │   └── ...
│   └── config.yaml
├── concepts/
│   └── mapping.csv                # CIEL to OMOP mappings
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
└── docker-compose.yml
```

## Data Quality

Pipeline validated using OHDSI tools:
- **Achilles**: Automated data characterization
- **Data Quality Dashboard**: Completeness, conformance, plausibility checks

## References

- [OMOP CDM v5.4 Documentation](https://ohdsi.github.io/CommonDataModel/)
- [OpenMRS Data Model](https://wiki.openmrs.org/display/docs/Data+Model)
- [SQLMesh Documentation](https://sqlmesh.readthedocs.io/)
- [OHDSI Tools](https://www.ohdsi.org/software-tools/)