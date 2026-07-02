# Employee Payroll Data Pipeline — AWS Reference Project
 
A production-style data engineering pipeline built on AWS, modeled on real patterns from the Industowers I-DoT Data Platform internship.
 
## Architecture
 
```
Amazon RDS (PostgreSQL)
        │
        ▼
AWS DMS (Full Load + CDC)
        │
        ▼
Amazon MSK / Kafka (EC2)
        │
        ├──────────────────────────────────┐
        ▼                                  ▼ (Future Phase)
AWS Glue ETL                        Spark Streaming / Flink
        │                                  │
        ▼                                  ▼
S3 Bronze (raw, Hudi)           Amazon Redshift Serverless
        │
        ▼
AWS Glue ETL (clean, dedupe)
        │
        ▼
S3 Silver (cleaned, Hudi)
        │
        ▼
AWS Glue ETL (aggregate)
        │
        ▼
S3 Gold (aggregated, Parquet)
        │
        ▼
AWS Glue Data Catalog
        │
        ▼
Amazon Athena ──► Amazon QuickSight
```
 
## Tech Stack
 
| Layer | Technology | Why |
|---|---|---|
| Source DB | Amazon RDS (PostgreSQL) | WAL-based logical replication for lossless CDC |
| CDC | AWS DMS (Full Load + CDC) | Managed, no-trigger CDC via replication slots |
| Streaming | Kafka on EC2 (MSK in production) | Durable, replayable log; multiple independent consumers |
| Ingestion ETL | AWS Glue Streaming (Spark) | Serverless Spark; native S3/Hudi integration |
| Bronze Storage | Apache Hudi on S3 | Row-level upserts for CDC UPDATE/DELETE operations |
| Cleaning ETL | AWS Glue Batch (Spark) | Separate job for clean/dedupe logic |
| Silver Storage | Apache Hudi on S3 | Upsert-safe cleaned layer |
| Aggregation ETL | AWS Glue Batch (Spark) | Department-level payroll summaries |
| Gold Storage | Parquet on S3 | Read-optimized, no upserts needed at this layer |
| Schema Registry | AWS Glue Data Catalog | Single schema source for Athena + Glue jobs |
| Governance | AWS Lake Formation | Column/row-level PII access control |
| Query Layer | Amazon Athena | Serverless, pay-per-query SQL on Gold |
| Visualization | Amazon QuickSight | Native Athena integration, AWS-native BI |
 
## Medallion Architecture (Bronze / Silver / Gold)
 
- **Bronze**: Raw, unmodified data as received from DMS/Kafka. Preserved for audit and replay. Uses Hudi for CDC upsert support.
- **Silver**: Cleaned, deduplicated, schema-conformed data. Business rules applied. Also Hudi for safe upserts.
- **Gold**: Department-level payroll aggregates (headcount, avg salary, total payroll cost). Plain Parquet — write-once, read-many.
## Pipeline Verified End-to-End
 
- Full Load: 10 employee records loaded from RDS into Kafka ✅
- CDC: Live UPDATE on salary captured and delivered to Kafka in real time ✅
- Bronze: Hudi table populated from Kafka stream ✅
- Silver: Cleaned/deduped Hudi table from Bronze ✅
- Gold: Department payroll summary Parquet from Silver ✅
- Athena: SQL query on Gold returning correct aggregates ✅
## Sample Athena Query Result
 
```sql
SELECT * FROM gold_department_payroll_summary;
```
 
| department | headcount | avg_salary | total_payroll_cost |
|---|---|---|---|
| ENGINEERING | 3 | 83333.33 | 250000.0 |
| FINANCE | 2 | 74000.0 | 148000.0 |
| HR | 2 | 60000.0 | 120000.0 |
| MARKETING | 1 | 68000.0 | 68000.0 |
| SALES | 2 | 62500.0 | 125000.0 |
 
## AWS Resources
 
- **Region**: ap-south-1 (Mumbai)
- **RDS Endpoint**: `payroll-db.cfqw62qeaem2.ap-south-1.rds.amazonaws.com`
- **S3 Buckets**:
  - `ujjawal-payroll-bronze-2026-406430963255-ap-south-1`
  - `ujjawal-payroll-silver-2026-406430963255-ap-south-1`
  - `ujjawal-payroll-gold-2026-406430963255-ap-south-1`
- **IAM Role**: `payroll-glue-role`
- **Kafka**: Running on EC2 (`kafka-host`) via Docker Compose in KRaft mode
## Project Structure
 
```
employee-payroll-pipeline/
├── glue-jobs/
│   ├── kafka_to_bronze.py       # Streaming: Kafka → S3 Bronze (Hudi)
│   ├── bronze_to_silver.py      # Batch: Bronze → Silver (clean, dedupe)
│   └── silver_to_gold.py        # Batch: Silver → Gold (aggregate, Parquet)
├── kafka-local/
│   └── docker-compose.yml       # Local Kafka (KRaft mode, no Zookeeper)
├── sample-data/
│   ├── 01_create_employees_table.sql
│   └── 02_seed_employees.sql
└── docs/
    └── Architecture_Justification_Document.docx
```
 



































