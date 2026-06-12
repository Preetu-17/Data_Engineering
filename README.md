# Medallion Data Lakehouse with Apache Data Engineering Stack

## 📌 Overview
This repository demonstrates a production-grade, end-to-end data engineering pipeline built on a modern Apache-based 
open lakehouse architecture. 

The project implements a **Medallion Architecture (Bronze, Silver, and Gold)** and an **Operations Layer** for observability and governance. Data is concurrently stored using both **Delta Lake** and **Apache Iceberg** table formats backed by Parquet storage. 

The entire lifecycle—from raw CSV ingestion through business-ready analytics, data quality monitoring, audit logging, and lineage tracking—is orchestrated using Apache Airflow and implemented using PySpark.

This solution simulates a real-world enterprise data platform focusing on:

Data Ingestion & Processing
Lakehouse Architecture
Data Quality Validation
Auditability & Traceability
Data Lineage & Governance
Workflow Orchestration
Multi-Format Storage (Delta Lake & Iceberg)

```
🏗️ Solution Architecture

                 +-------------------+
                 |   Source CSV Data |
                 +-------------------+
                           |
                           v
                 +-------------------+
                 |      Bronze       |
                 |   Raw Ingestion   |
                 +-------------------+
                           |
                           v
                 +-------------------+
                 |      Silver       |
                 | Cleansed & Joined |
                 +-------------------+
                           |
                           v
                 +-------------------+
                 |       Gold        |
                 | Business Metrics  |
                 +-------------------+
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
 +----------------+ +----------------+ +----------------+
 | Audit Logging  | | Data Quality   | | Data Lineage   |
 | & Monitoring   | | Validation     | | Tracking       |
 +----------------+ +----------------+ +----------------+
          ```


## 🛠️ Technology Stack
*   **Orchestration:** Apache Airflow 2.9.3 (Manages the daily pipeline DAG and triggers Spark tasks)
*   **Data/Streaming Ingestion:** Apache Kafka 3.7.0 (Ingests the path or raw rows of incoming CSV data)
*   **Data Processing Engine:** Apache Spark 3.5.1 (Scala 2.12 bundle)
*   **Storage Frameworks:** Delta Lake 3.2.0 and Apache Iceberg 1.4.3 (Using the Spark runtime JAR)
*   **Data Layout:** Medallion Architecture (Bronze, Silver, and Gold folders on local file system)
*   **Language:** PySpark (Python 3.12)

## 📁 Directory Architecture

Data_Lake/
├── bronze/     # Appended raw data in Delta & Iceberg format
├── silver/     # Cleaned, deduplicated Delta & Iceberg tables
└── gold/       # Aggregated business metrics in Delta & Iceberg format
└── Operations
│── pipeline_execution_log/
│             └── Audit Metrics 
├── data_quality_metrics/
│             └── Data Profiling Results 
├── table_lineage/ 
|            └── Source-to-Target Lineage
│
├── audit_job.py
├── data_quality_job.py
└── lineage_job.py

## 🌟 Medallion Architecture Layers

### 🟫 Bronze Layer (Raw Ingestion)
*   **Purpose:** To ingest and store raw source data exactly as it arrives.
*   **Key Operations:** Minimal transformation, schema preservation, and immutable historical ledgering.

### ⬜ Silver Layer (Enriched & Conformed)
*   **Purpose:** To create an enterprise "Single Source of Truth" by refining raw inputs.
*   **Key Operations:** Data cleansing, schema standardization, row deduplication, and initial business rule application.

### 🟨 Gold Layer (Actionable Insights)
*   **Purpose:** To deliver highly optimized, business-ready datasets tailored for analytics.
*   **Key Operations:** High-level aggregations, dimensional reporting tables, and cross-departmental KPI generation.

###🔍 Operations Layer (Data Observability)
The Operations Layer provides enterprise-grade monitoring, governance, and traceability capabilities.

1️⃣ Audit Logging

Script:audit_job.py
Output:
Operations/pipeline_execution_log

Captured Metrics:
Layer Name
Table Name
Row Count
Column Count
Schema Definition
Processing Status
Execution Timestamp

Benefits:
Pipeline Monitoring
Audit Trail
Operational Visibility

2️⃣ Data Quality Monitoring

Script:data_quality_job.py
Output:
Operations/data_quality_metrics

Captured Metrics:
Row Count
Duplicate Count
Distinct Count
Null Value Count
Quality Status

Benefits:
Early Detection of Data Issues
Data Profiling
Trustworthy Analytics

3️⃣ Data Lineage Tracking

Script:lineage_job.py
Output:
Operations/table_lineage

Captured Metadata:
Source Layer
Source Table
Target Layer
Target Table
Lineage Creation Timestamp

Benefits:
End-to-End Traceability
Impact Analysis
Data Governance
Regulatory Compliance

🔄 Workflow Orchestration

Apache Airflow orchestrates the complete pipeline:

bronze_job.py
        ↓
silver_job.py
        ↓
gold_job.py
        ↓
 ┌──────────────┬──────────────┬
 │              │              │
 ▼              ▼              ▼
audit_job   data_quality   lineage_job

Features:

Dependency Management
Scheduling
Retry Logic
Monitoring
Failure Handling

## 💼 Business Use-Case & Value

The core architecture of this platform is built on an industry-standard data stack designed for scalability, fault tolerance, and high-performance operations. By decoupling ingestion (Kafka), compute (Spark), transaction layers (Iceberg/Delta), and scheduling (Airflow), this framework addresses critical enterprise data challenges.

This platform demonstrates how modern organizations can:

Build scalable Lakehouse architectures.
Enable self-service analytics.
Maintain complete auditability.
Track data lineage across layers.
Monitor data quality continuously.
Support governance and compliance requirements.
Deliver trusted analytics to business users.

### 🏗️ Enterprise Data Engineering (ETL / ELT)
*   **Unified Processing:** Seamlessly switches between streaming (real-time Kafka ingestion) and batch processing (scheduled Airflow pipelines) depending on business urgency.
*   **Format Agnostic:** Handles multi-format data mutations, transforming raw source files (like CSV, JSON, Parquet) into reliable, optimized lakehouse formats.
*   **Zero-Downtime Schema Evolution:** Automatically adjusts table schemas as business requirements change, eliminating the need to rebuild downstream tables.


## 👤 Author
**Preetham**  
Senior Data Engineer

*   **LinkedIn:** [linkedin.com/in/Preetham1791](https://linkedin.com/in/Preetham1791)  
*   **GitHub:** [github.com/Preetu-17](https://github.com/Preetu-17)  


## 📄 License
This project is for learning, demonstration, and portfolio purposes.
