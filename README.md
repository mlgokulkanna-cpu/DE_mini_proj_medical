# Medical Data Engineering Project

A comprehensive data engineering solution for healthcare analytics using the Medallion Architecture (Bronze-Silver-Gold) on Databricks. This project processes medical encounter data from Azure Blob Storage and transforms it into actionable insights through a dimensional data model.

## Table of Contents

* [Overview](#overview)
* [Architecture](#architecture)
* [Data Sources](#data-sources)
* [Data Layers](#data-layers)
* [Project Structure](#project-structure)
* [Setup Instructions](#setup-instructions)
* [Usage Guide](#usage-guide)
* [KPI Metrics](#kpi-metrics)
* [Technologies](#technologies)

---

## Overview

This project implements a scalable, cloud-based data engineering pipeline designed to process and analyze medical encounter data. The solution follows the Medallion Architecture pattern, progressively refining data from raw ingestion through curated analytics-ready datasets.

**Key Features:**
* Multi-layer data architecture (Bronze → Silver → Gold)
* Dimensional modeling for optimized analytics
* Automated data quality checks
* Pre-built KPI metrics for healthcare insights
* Scalable processing on Databricks platform

---

## Architecture

### Medallion Architecture

The project follows the Medallion Architecture pattern with three distinct layers:

```
┌─────────────────────────────────────────────────────────────┐
│                    Azure Blob Storage                        │
│         (patients, encounters, organizations,                │
│              payers, procedures)                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER - Raw Data Ingestion                          │
│  • Raw files ingested as-is                                 │
│  • No transformations applied                               │
│  • Historical data preserved                                │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER - Cleaned & Validated Data                    │
│  • 5 Core Tables:                                           │
│    - patients                                               │
│    - encounters                                             │
│    - organizations                                          │
│    - payers                                                 │
│    - procedures                                             │
│  • Data quality checks applied                              │
│  • Standardized schemas                                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD LAYER - Analytics-Ready Dimensional Model             │
│  • Fact Tables:                                             │
│    - fact_encounters                                        │
│    - fact_procedures                                        │
│  • Dimension Tables:                                        │
│    - dim_patients                                           │
│    - dim_payers                                             │
│    - dim_procedures                                         │
│    - dim_date                                               │
│  • KPI Tables (6 pre-built metrics)                        │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Sources

### Azure Blob Storage

All source data is stored in Azure Blob Storage with the following tables:

| Table | Description | Key Fields |
| --- | --- | --- |
| `patients` | Patient demographic information | patient_id, birth_date, gender, race, ethnicity |
| `encounters` | Medical encounter records | encounter_id, patient_id, organization_id, payer_id, encounter_class, start_date, end_date |
| `organizations` | Healthcare provider organizations | organization_id, name, address, city, state |
| `payers` | Insurance payer information | payer_id, name, coverage_type |
| `procedures` | Medical procedures performed | procedure_id, encounter_id, code, description, cost |

---

## Data Layers

### Bronze Layer (Raw Data)

**Purpose:** Raw data ingestion zone

* Ingests data directly from Azure Blob Storage
* Preserves source data in original format
* No transformations or cleaning applied
* Serves as immutable source of truth
* Enables data replay and reprocessing

**Tables:** `bronze_patients`, `bronze_encounters`, `bronze_organizations`, `bronze_payers`, `bronze_procedures`

### Silver Layer (Cleaned Data)

**Purpose:** Cleaned, validated, and standardized data

* Applies data quality rules and validations
* Removes duplicates and handles nulls
* Standardizes data types and formats
* Enriches data with derived fields
* Implements slowly changing dimensions (SCD) where applicable

**Tables:**
* `silver_patients` - Cleaned patient demographics
* `silver_encounters` - Validated encounter records
* `silver_organizations` - Standardized organization data
* `silver_payers` - Cleaned payer information
* `silver_procedures` - Validated procedure records

### Gold Layer (Analytics-Ready)

**Purpose:** Dimensional model optimized for analytics and reporting

#### Fact Tables

* **`fact_encounters`** - Grain: One row per encounter
  * Links to dimension tables via foreign keys
  * Contains measurable metrics (duration, cost, etc.)
  
* **`fact_procedures`** - Grain: One row per procedure
  * Links to encounter and procedure dimensions
  * Contains procedure-level metrics

#### Dimension Tables

* **`dim_patients`** - Patient dimension with demographic attributes
* **`dim_payers`** - Payer/insurance dimension
* **`dim_procedures`** - Procedure type dimension
* **`dim_date`** - Date dimension with calendar hierarchies (year, quarter, month, day)

#### KPI Tables

Six pre-aggregated KPI tables for optimized reporting performance (see [KPI Metrics](#kpi-metrics) section).

---

## Project Structure

```
DE_mini_proj_medical/
│
├── README.md                          # This file
│
├── notebooks/
│   ├── bronze/
│   │   ├── 01_ingest_patients.py      # Ingest patients data
│   │   ├── 02_ingest_encounters.py    # Ingest encounters data
│   │   ├── 03_ingest_organizations.py # Ingest organizations data
│   │   ├── 04_ingest_payers.py        # Ingest payers data
│   │   └── 05_ingest_procedures.py    # Ingest procedures data
│   │
│   ├── silver/
│   │   ├── 01_clean_patients.py       # Clean and validate patients
│   │   ├── 02_clean_encounters.py     # Clean and validate encounters
│   │   ├── 03_clean_organizations.py  # Clean and validate organizations
│   │   ├── 04_clean_payers.py         # Clean and validate payers
│   │   └── 05_clean_procedures.py     # Clean and validate procedures
│   │
│   ├── gold/
│   │   ├── 01_create_dimensions.py    # Build dimension tables
│   │   ├── 02_create_facts.py         # Build fact tables
│   │   └── 03_create_kpis.py          # Generate KPI tables



---

## Setup Instructions

### Prerequisites

* Databricks workspace (AWS)
* Azure Blob Storage account with medical data files
* Unity Catalog enabled
* Appropriate IAM permissions for Azure Blob Storage access

### Step 1: Configure Azure Blob Storage Access

1. Create an Azure Storage Account if not already available
2. Configure Databricks access to Azure Blob Storage:
   ```python
   # Option 1: Using Service Principal
   spark.conf.set("fs.azure.account.auth.type", "OAuth")
   spark.conf.set("fs.azure.account.oauth.provider.type", 
                  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
   spark.conf.set("fs.azure.account.oauth2.client.id", "<client-id>")
   spark.conf.set("fs.azure.account.oauth2.client.secret", "<client-secret>")
   spark.conf.set("fs.azure.account.oauth2.client.endpoint", 
                  "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
   
   # Option 2: Using SAS Token
   spark.conf.set("fs.azure.sas.<container>.<storage-account>.blob.core.windows.net", 
                  "<sas-token>")
   ```



## KPI Metrics

The Gold layer includes six pre-aggregated KPI tables for optimized reporting:

### KPI 1: Encounter Mix Analysis
**Table:** `gold.kpi_encounter_mix`

**Description:** Analyzes the distribution of encounter types (inpatient, outpatient, emergency, etc.) by year and quarter.

**Metrics:**
* Encounter count by class
* Percentage distribution
* Year-over-year growth
* Quarterly trends

**Use Cases:**
* Capacity planning
* Resource allocation
* Seasonal pattern identification

**Query Example:**
```sql
SELECT 
    year,
    quarter,
    encounter_class,
    encounter_count,
    percentage_of_total,
    yoy_growth_rate
FROM medical_data.gold.kpi_encounter_mix
WHERE year = 2023
ORDER BY quarter, encounter_count DESC;
```

### KPI 2: Patient Demographics Summary
**Table:** `gold.kpi_patient_demographics`

**Description:** Aggregated patient demographics including age groups, gender, race, and ethnicity distributions.

**Metrics:**
* Patient count by demographic segment
* Age distribution (pediatric, adult, senior)
* Gender breakdown
* Race and ethnicity statistics

**Use Cases:**
* Population health management
* Health equity analysis
* Targeted care programs

### KPI 3: Payer Performance Metrics
**Table:** `gold.kpi_payer_performance`

**Description:** Financial and operational metrics by insurance payer.

**Metrics:**
* Total claims processed
* Average claim cost
* Claim approval rate
* Revenue by payer
* Encounter volume by payer

**Use Cases:**
* Contract negotiations
* Revenue cycle management
* Payer mix optimization

### KPI 4: Procedure Volume & Cost Analysis
**Table:** `gold.kpi_procedure_analytics`

**Description:** Tracks procedure volumes, costs, and trends over time.

**Metrics:**
* Procedure count by type
* Average procedure cost
* Total procedure revenue
* Cost trends over time
* High-cost procedure identification

**Use Cases:**
* Clinical efficiency analysis
* Cost management
* Service line planning

### KPI 5: Encounter Duration Analysis
**Table:** `gold.kpi_encounter_duration`

**Description:** Analyzes the length of stay and encounter durations.

**Metrics:**
* Average encounter duration by class
* Median length of stay
* Duration distribution percentiles
* Outlier identification

**Use Cases:**
* Operational efficiency
* Bed management
* Care pathway optimization

### KPI 6: Monthly Trend Dashboard
**Table:** `gold.kpi_monthly_trends`

**Description:** Month-over-month key metrics for executive dashboards.

**Metrics:**
* Total encounters
* Total revenue
* Average cost per encounter
* Patient volume
* Month-over-month changes

**Use Cases:**
* Executive reporting
* Performance tracking
* Budget variance analysis

---

## Technologies

* **Cloud Platform:** Databricks on AWS
* **Storage:** Azure Blob Storage
* **Catalog:** Unity Catalog
* **Processing Engine:** Apache Spark
* **Languages:** Python, SQL
* **Architecture Pattern:** Medallion Architecture (Bronze-Silver-Gold)

---

## Data Quality

The pipeline implements comprehensive data quality checks at each layer:

* **Bronze Layer:** Schema validation, file completeness
* **Silver Layer:** Null checks, referential integrity, duplicate detection, format validation
* **Gold Layer:** Business rule validation, dimensional integrity, metric accuracy

---



## Contributing

To contribute to this project:

1. Create feature branch from `main`
2. Implement changes with appropriate tests
3. Update documentation as needed
4. Submit pull request with detailed description

