# Data Governance, Classification & Compliance Framework
**Document Version:** 1.0.0  
**Effective Date:** 2026-09-01  
**Entity:** Presight AI — Big Data Engineering Platform  
**Compliance Mandates:** UAE Federal Decree-Law No. 45/2021 (PDPL) & EU GDPR  

---

## 1. Data Classification Policy & Tiering Matrix

All datasets processed across ETL pipelines, analytical star schemas, and streaming infrastructure are classified into three security levels to govern ingestion, transformation, storage, and retention.

| Tier | Category | Scope / Datasets | Examples | Storage & Encryption Controls | Access Authorization |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Tier 1** | **Highly Confidential (PII)** | `employees`, `employees_salary_history` | Employee Full Name, Email, Phone, Base Salary, Bonus, National ID | AES-256 at rest, TLS 1.3 in transit, Dynamic masking / pseudonymization | Role-Based Access Control (RBAC): HR Admin, Security Officer only |
| **Tier 2** | **Confidential (Business Sensitive)** | `transactions`, `projects` | Budget, Approved Amount, Client Name, Hourly Rate, Project Margin | Server-side encryption (SSE-S3 / encrypted EBS), volume-level encryption | Project Managers, Finance Controllers, Data Engineers |
| **Tier 3** | **Internal Operational** | `events_stream`, logs | System events, logins, document uploads, system metrics | Standard object store encryption, compressed Parquet partitions | Platform Engineers, BI Analysts, Operations Team |

---

## 2. Field-Level Inventory & PII Masking Architecture

### 2.1 PII Inventory
* `employee_id`: Pseudonymous natural key.
* `full_name`: Direct identifier. Masked using deterministic SHA-256 with salt in lower environments.
* `email`: Direct identifier. Masked as `a***@presight.ai` for non-HR analytical workloads.
* `salary`: Sensitive personal/financial data. Only exposed as aggregated brackets (`salary_band`) in dimensional models.

### 2.2 In-Transit and At-Rest Encryption Architecture
* **In-Flight:** All inter-service communications (Airflow workers, Kafka broker listeners `PLAINTEXT_INTERNAL://kafka:29092`, PostgreSQL metadata connections) enforce TLS 1.3.
* **At-Rest:** 
  * Docker volume storage (`postgres_data`, output mounts) mapped to encrypted underlying host storage.
  * Column-level encryption applied to financial amounts and wage logs in downstream warehousing.

---

## 3. Regulatory Alignment: UAE PDPL & EU GDPR

### 3.1 UAE Federal Decree-Law No. 45/2021 on Personal Data Protection (PDPL)
* **Article 5 (Data Processing Principles):** Processing of employee records is restricted to legitimate business and payroll optimization purposes. Data minimization is enforced at ingestion.
* **Article 13 (Right to Erasure / Right to be Forgotten):**
  * Automated deletion workflows target operational staging tables upon verified deletion requests.
  * In append-only SCD Type 2 tables (`dim_employee`), historical snapshots are anonymized (`full_name = 'REDACTED'`, `email = 'redacted@anonymous.local'`) while retaining numerical foreign keys to preserve financial auditability.
* **Cross-Border Transfer Restrictions:** All transactional and employee data residing within UAE jurisdiction cannot be routed to international cloud zones without statutory adequacy validation.

### 3.2 EU GDPR General Data Protection Regulation
* **Article 6 & 9 (Lawful Basis & Special Categories):** Processing grounded in contract fulfillment and legitimate operational interest.
* **Article 25 (Data Protection by Design & by Default):** Automated Data Quality gates in Airflow (`validate_data_quality`) and schema validators discard unverified and unauthorized attributes prior to warehouse persistence.
* **Article 30 (Records of Processing Activities - ROPA):** Every DAG execution generates a tamper-evident audit record (`outputs/pipeline_report_<date>.txt`) tracking source volumes, ingestion timestamps, and transformed row counts.

---

## 4. Data Lifecycle, Retention & Disposal Schedules

| Dataset | Ingestion Stage | Active Retention Period | Cold Archive (Parquet/Glacier) | Final Disposal Method |
| :--- | :--- | :--- | :--- | :--- |
| `events_stream` (Kafka / JSONL) | Streaming / Real-Time | 7 Days (Kafka buffer) | 365 Days partitioned by `event_date` | Automated TTL lifecycle policy deletion |
| `transactions` | Batch / Daily Incremental | 7 Years (Financial audit compliance) | Indefinite read-only archive | Cryptographic erasure of archive encryption keys |
| `employees_salary_history` | SCD Type 2 Batch | Duration of active employment + 5 Years | 10 Years immutable storage | Purged via database record tombstone & vacuum |
| `projects` | Batch / Daily Incremental | 5 Years post project delivery | 10 Years archive | Standard secure sector overwrite |

---

## 5. Automated Data Quality Gate & Incident Response Playbook

1. **Gate Thresholds:** Completeness below 80% on primary identifiers (`project_id`, `employee_id`, `transaction_id`) automatically fails the orchestration DAG and triggers critical alerts.
2. **Breach Notification:** In the event of unauthorized access to Tier 1 records, notification procedures to the UAE Data Office and impacted individuals are initiated within 72 hours in strict accordance with UAE PDPL Article 9.