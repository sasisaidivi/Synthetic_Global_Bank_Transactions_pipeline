# 🏦 Synthetic Global Bank Transactions Data Pipeline

> **An end-to-end cloud data engineering project that demonstrates how different types of banking data require different loading strategies.**

This project builds a production-inspired data pipeline for a synthetic global banking environment using **Python, Apache Airflow, PySpark, Google Cloud Storage, BigQuery, and Google Cloud Dataproc**.

Rather than applying the same ETL approach to every dataset, the pipeline uses the loading strategy that best fits each type of data:

* 🔄 **Transactions → Incremental Load**
* 🔁 **Categories → Full Load / Overwrite**
* 🔀 **Clients → Upsert + SCD Type 2**
* 🔀 **Subscriptions → Upsert + SCD Type 2**

The goal is to simulate a more realistic data engineering environment where transactional and master data evolve differently and therefore require different data processing strategies.

---

# 📌 Table of Contents

* [Project Overview](#-project-overview)
* [The Business Problem](#-the-business-problem)
* [Why Different Load Strategies?](#-why-different-load-strategies)
* [Pipeline Architecture](#️-pipeline-architecture)
* [Data Loading Strategies](#-data-loading-strategies)
* [SCD Type 2 Implementation](#-scd-type-2-implementation)
* [Technology Stack](#️-technology-stack)
* [Project Structure](#-project-structure)
* [Data Pipeline Workflow](#-data-pipeline-workflow)
* [Deployment Guide](#-deployment-guide)
* [Data Quality and Validation](#-data-quality-and-validation)
* [Key Engineering Concepts](#-key-engineering-concepts)
* [Future Improvements](#-future-improvements)

---

# 🚀 Project Overview

Modern banking systems generate different types of data.

Some data grows continuously.

For example, **transactions** are constantly being created. Reloading the entire transaction history every time would become expensive and inefficient.

Other datasets, such as **categories**, are relatively small and can be safely refreshed completely.

Customer-related entities such as **clients** and **subscriptions** are different again. Their records can change over time, and in many cases, the business needs to preserve historical information.

This project addresses those challenges by implementing multiple data loading patterns in a single end-to-end pipeline.

```text
Different Data Types
        │
        ▼
Different Loading Strategies
        │
        ▼
Reliable & Scalable Data Pipeline
```

---

# 🎯 The Business Problem

Imagine a global bank with multiple operational datasets.

The bank receives data related to:

* 💳 Customer transactions
* 👥 Clients
* 📂 Transaction categories
* 📄 Banking subscriptions

A simple approach would be to truncate every table and reload everything.

However, that approach creates several problems:

❌ Expensive for large transaction datasets
❌ Slow pipeline execution
❌ Loss of historical changes
❌ Unnecessary data processing
❌ Poor scalability

A better solution is to select the loading strategy based on how the data behaves.

This project implements exactly that approach.

---

# 🧠 Why Different Load Strategies?

| Dataset          | Loading Strategy      | Reason                                                      |
| ---------------- | --------------------- | ----------------------------------------------------------- |
| 💳 Transactions  | Incremental Load      | Transaction data continuously grows                         |
| 📂 Categories    | Full Load / Overwrite | Small reference data that can be refreshed completely       |
| 👥 Clients       | Upsert + SCD Type 2   | Client information can change and history must be preserved |
| 📄 Subscriptions | Upsert + SCD Type 2   | Subscription details can change over time                   |

This design reflects how real-world data engineering pipelines are built.

---

# 🏗️ Pipeline Architecture

The pipeline follows an end-to-end cloud data engineering architecture.

```text
                    ┌─────────────────────┐
                    │   Source Datasets   │
                    │                     │
                    │ Transactions        │
                    │ Categories          │
                    │ Clients             │
                    │ Subscriptions       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Data Ingestion      │
                    │ Python / PySpark    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Google Cloud        │
                    │ Storage (GCS)       │
                    │ Raw Data Layer      │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Processing Layer    │
                    │ PySpark / Dataproc  │
                    └──────────┬──────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │     Data Loading Strategies    │
              ├────────────────────────────────┤
              │ Transactions → Incremental     │
              │ Categories   → Full Refresh    │
              │ Clients      → Upsert + SCD2   │
              │ Subscriptions→ Upsert + SCD2   │
              └───────────────┬────────────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │ BigQuery            │
                    │ Analytics Warehouse │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ KPIs & Analytics    │
                    └─────────────────────┘
```

---

# 🔄 Data Loading Strategies

## 💳 1. Transactions — Incremental Load

Transactions are continuously generated.

Because the transaction table grows over time, performing a complete reload for every pipeline execution would be inefficient.

This pipeline uses an **incremental loading strategy**.

### How It Works

The pipeline:

1. Identifies newly available transaction records.
2. Loads only the new data.
3. Avoids reprocessing existing records unnecessarily.
4. Appends new transactions to the destination.

```text
Existing Transactions
        │
        ▼
Last Processed Record
        │
        ▼
Identify New Transactions
        │
        ▼
Load Only New Records
        │
        ▼
Updated Transaction Table
```

### Why Incremental Loading?

✅ Faster execution
✅ Reduced processing cost
✅ Better scalability
✅ Suitable for continuously growing datasets
✅ Avoids unnecessary full-table processing

---

# 📂 2. Categories — Full Load / Overwrite

Categories are reference or lookup data.

Because this dataset is relatively small, the pipeline performs a **full refresh** during every execution.

The existing target data is replaced with the latest source version.

```text
Latest Categories Dataset
           │
           ▼
     Full Load
           │
           ▼
Overwrite Existing Table
           │
           ▼
Updated Categories Table
```

### Why Full Load?

For small reference datasets:

* The processing cost is low.
* The logic is simple.
* The destination always contains the latest complete version.

This makes a full refresh an appropriate strategy.

---

# 👥 3. Clients — Upsert with SCD Type 2

Client data can change over time.

For example:

* A client may move to another country.
* Contact information may change.
* Account-related attributes may change.

Simply overwriting the old record would remove valuable historical information.

To solve this problem, the pipeline uses:

> **Upsert Logic + Slowly Changing Dimension Type 2 (SCD Type 2)**

---

# 🕒 What Is SCD Type 2?

SCD Type 2 is a data warehousing technique used to preserve historical changes.

Instead of replacing an existing record, the old version is closed and a new version is created.

### Example

Suppose a client changes their country.

### Original Record

| Client ID | Country | Start Date | End Date | Current |
| --------- | ------- | ---------- | -------- | ------- |
| 101       | India   | 2024-01-01 | NULL     | Yes     |

### Client Changes Country

Instead of overwriting the record:

### Historical Record

| Client ID | Country | Start Date | End Date   | Current |
| --------- | ------- | ---------- | ---------- | ------- |
| 101       | India   | 2024-01-01 | 2025-06-15 | No      |

### New Current Record

| Client ID | Country | Start Date | End Date | Current |
| --------- | ------- | ---------- | -------- | ------- |
| 101       | USA     | 2025-06-16 | NULL     | Yes     |

This allows the warehouse to answer questions such as:

> **What was the client's information at a specific point in time?**

---

# 📄 4. Subscriptions — Upsert with SCD Type 2

Subscription information can also change over time.

Examples include:

* Subscription plan changes
* Status changes
* Subscription upgrades
* Subscription cancellations
* Other business attribute changes

The pipeline therefore uses the same historical approach:

> **Upsert + SCD Type 2**

### Pipeline Behavior

#### 🆕 New Subscription

```text
New Record
    │
    ▼
Insert into Warehouse
```

#### 🔄 Existing Subscription With Changes

```text
Existing Record
       │
       ▼
Change Detected
       │
       ├── Close Previous Version
       │
       └── Insert New Version
```

#### ✅ Existing Subscription Without Changes

```text
Existing Record
       │
       ▼
No Change Detected
       │
       ▼
No New Version Required
```

---

# 🔀 Understanding the Upsert Logic

The upsert process handles two major scenarios:

## INSERT

If the record does not already exist:

```text
Source Record
      │
      ▼
Not Found in Target
      │
      ▼
INSERT
```

## UPDATE

If the business record already exists:

```text
Source Record
      │
      ▼
Found in Target
      │
      ▼
Compare Attributes
      │
      ├── No Change → Keep Current Record
      │
      └── Change → Apply SCD Type 2
```

---

# ⚙️ Pipeline Orchestration

The pipeline is organized to support automated execution and orchestration.

The repository contains a dedicated pipeline structure including:

```text
data_pipeline/
│
├── config/
├── dags/
└── scripts/
```

Apache Airflow is used as part of the project's orchestration environment.

The pipeline can coordinate tasks such as:

```text
Start Pipeline
      │
      ▼
Read Source Data
      │
      ▼
Upload / Access Cloud Storage
      │
      ▼
Validate Data
      │
      ▼
Process Transactions
      │
      ▼
Process Categories
      │
      ▼
Process Clients
      │
      ▼
Process Subscriptions
      │
      ▼
Load BigQuery
      │
      ▼
Run KPI Queries
      │
      ▼
Pipeline Complete
```

---

# ☁️ Google Cloud Architecture

The project uses Google Cloud services for storage, processing, and analytics.

## 🪣 Google Cloud Storage

Used as the data storage layer.

Typical responsibilities include:

* Raw data storage
* Intermediate pipeline storage
* Input datasets
* Processed data files

---

## 🔥 Google Cloud Dataproc

Used to support scalable data processing using **PySpark**.

PySpark is suitable for:

* Large-scale transformations
* Distributed processing
* Data cleaning
* Data preparation
* Business rule implementation

---

## 📊 Google BigQuery

BigQuery acts as the analytics and data warehouse layer.

It stores the processed datasets and supports:

* SQL analytics
* KPI calculations
* Historical analysis
* Reporting
* Business intelligence

---

# 📊 Data Pipeline Workflow

The complete workflow can be summarized as:

```text
┌─────────────────────────┐
│ Synthetic Banking Data  │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ Data Ingestion          │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ Google Cloud Storage    │
│ Raw Data Layer          │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ PySpark Processing      │
│ Google Cloud Dataproc   │
└────────────┬────────────┘
             │
             ▼
┌──────────────────────────────────┐
│ Entity-Specific Loading Strategy │
├──────────────────────────────────┤
│ Transactions → Incremental Load  │
│ Categories → Full Load           │
│ Clients → Upsert + SCD Type 2    │
│ Subscriptions → Upsert + SCD2    │
└────────────┬─────────────────────┘
             │
             ▼
┌─────────────────────────┐
│ BigQuery Data Warehouse │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ KPIs & Business Insights│
└─────────────────────────┘
```

---

# 🛠️ Technology Stack

| Technology               | Purpose                           |
| ------------------------ | --------------------------------- |
| 🐍 Python                | Pipeline development              |
| 🔥 PySpark               | Distributed data processing       |
| ⚙️ Apache Airflow        | Workflow orchestration            |
| ☁️ Google Cloud Platform | Cloud infrastructure              |
| 🪣 Google Cloud Storage  | Data storage                      |
| 🔥 Google Cloud Dataproc | Spark processing                  |
| 📊 BigQuery              | Data warehouse and analytics      |
| 📝 SQL                   | Data transformation and analytics |
| 📦 UV                    | Python dependency management      |

---

# 📁 Project Structure

```text
Synthetic_Global_Bank_Transactions_pipeline/
│
├── data_pipeline/
│   ├── config/
│   │   └── Pipeline configuration
│   │
│   ├── dags/
│   │   └── Apache Airflow DAGs
│   │
│   └── scripts/
│       └── Pipeline execution scripts
│
├── data_sets/
│   └── Synthetic banking datasets
│
├── schemas/
│   ├── schema/
│   └── schema_string/
│
├── src/
│   └── synthetic_global_bank_transactions_pipeline/
│
├── gcp_env_set.py
│   └── GCP environment configuration
│
├── kpi_for_Synthetic_Global_Bank_Transactions.txt
│   └── KPI definitions and analytics
│
├── pyproject.toml
│   └── Project dependencies
│
├── uv.lock
│   └── Locked dependency versions
│
└── README.md
```

---

# 🚀 Deployment Guide

## 1️⃣ Prerequisites

Before deploying the pipeline, make sure you have:

* Python 3.12+
* Google Cloud Platform account
* Google Cloud SDK
* Google Cloud Storage access
* BigQuery enabled
* Dataproc enabled
* Required IAM permissions

---

## 2️⃣ Clone the Repository

```bash
git clone https://github.com/sasisaidivi/Synthetic_Global_Bank_Transactions_pipeline.git
```

Move into the project directory:

```bash
cd Synthetic_Global_Bank_Transactions_pipeline
```

---

## 3️⃣ Install Dependencies

This project uses `uv` for dependency management.

Install dependencies:

```bash
uv sync
```

Alternatively, install the project using Python tools available in your environment.

The project includes dependencies for:

* Apache Airflow
* Google Cloud Storage
* Google BigQuery
* Google Cloud Dataproc
* PySpark

---

## 4️⃣ Authenticate with Google Cloud

Login to Google Cloud:

```bash
gcloud auth login
```

Set your project:

```bash
gcloud config set project YOUR_PROJECT_ID
```

Verify the configuration:

```bash
gcloud config list
```

---

## 5️⃣ Enable Required GCP Services

Enable the required services:

```bash
gcloud services enable storage.googleapis.com
```

```bash
gcloud services enable bigquery.googleapis.com
```

```bash
gcloud services enable dataproc.googleapis.com
```

---

## 6️⃣ Create a Google Cloud Storage Bucket

Create a bucket for pipeline storage:

```bash
gcloud storage buckets create gs://YOUR_BUCKET_NAME \
    --location=YOUR_REGION
```

Example structure:

```text
YOUR_BUCKET_NAME/
│
├── raw/
├── processed/
└── temp/
```

---

## 7️⃣ Configure Authentication

For local development, configure Google Application Credentials.

### Linux/macOS

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Windows PowerShell

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service-account.json"
```

> ⚠️ Never commit service account keys or credentials to GitHub.

---

## 8️⃣ Configure the GCP Environment

Update the required project variables, such as:

```text
PROJECT_ID
BUCKET_NAME
BIGQUERY_DATASET
REGION
```

The repository also includes:

```text
gcp_env_set.py
```

Use this configuration according to your GCP environment setup.

---

## 9️⃣ Configure Apache Airflow

The Airflow pipeline files are located inside:

```text
data_pipeline/dags/
```

Configure your Airflow environment and ensure the DAG files are available to the Airflow scheduler.

Typical commands include:

```bash
airflow db migrate
```

Start the scheduler:

```bash
airflow scheduler
```

List available DAGs:

```bash
airflow dags list
```

Then trigger the required pipeline DAG.

---

# 🔍 Data Quality and Validation

A reliable pipeline should validate data before loading it into the warehouse.

Recommended validation checks include:

### 📊 Record Validation

* Source record count
* Target record count
* Incremental record count

### 🧹 Data Quality

* Null value checks
* Duplicate detection
* Schema validation
* Data type validation

### 🔑 Business Key Validation

Ensure that important business keys are correctly handled for:

* Transactions
* Clients
* Categories
* Subscriptions

### 🕒 SCD Type 2 Validation

For Clients and Subscriptions:

* Only one current record should exist per business key.
* Historical records should be marked as inactive.
* Start and end dates should correctly represent the history.
* New versions should be created only when tracked attributes change.

---

# 📈 Analytics and KPIs

After the pipeline loads the data into BigQuery, the warehouse can support banking analytics and KPI reporting.

Possible analysis areas include:

### 💳 Transaction Analytics

* Total transaction volume
* Transaction trends
* Transaction amount analysis
* Transaction frequency

### 👥 Client Analytics

* Active clients
* Client growth
* Historical client changes

### 📄 Subscription Analytics

* Active subscriptions
* Subscription changes
* Subscription lifecycle analysis

### 📊 Category Analytics

* Transaction distribution by category
* Category performance
* Spending patterns

---

# 🌟 What Makes This Project Different?

This project is not simply a pipeline that performs the same operation on every dataset.

The key design decision is:

> **The loading strategy is selected based on how each dataset behaves.**

### Transactions

📈 Data continuously grows → **Incremental Load**

### Categories

📂 Small reference dataset → **Full Refresh**

### Clients

👥 Attributes change over time → **Upsert + SCD Type 2**

### Subscriptions

📄 Business state changes over time → **Upsert + SCD Type 2**

This approach demonstrates an important real-world data engineering principle:

> **There is no single loading strategy that works best for every dataset.**

---

# 🎓 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* End-to-End Data Pipelines
* Incremental Data Loading
* Full Refresh Pipelines
* Upsert Operations
* Slowly Changing Dimensions
* SCD Type 2
* Historical Data Tracking
* Apache Airflow
* Workflow Orchestration
* PySpark
* Cloud Data Processing
* Google Cloud Storage
* Google Cloud Dataproc
* BigQuery
* Data Warehousing
* Data Quality Validation
* SQL Analytics

---

# 🔮 Future Improvements

Potential improvements include:

* 🔄 Automated watermark management for incremental loads
* 🧪 Automated data quality testing
* 🚨 Pipeline failure notifications
* 📊 Data quality dashboards
* 🔔 Alerting and monitoring
* 🐳 Docker containerization
* 🚀 CI/CD with GitHub Actions
* 🔐 Secret Manager integration
* 📈 BI dashboards using Power BI or Looker
* 📡 Streaming transaction ingestion
* 📊 Data lineage and observability

---

# 👨‍💻 Author

**Sasi Sai Divi**

Data Engineer | Python | SQL | PySpark
Google Cloud Platform | BigQuery | Apache Airflow

---

## ⭐ Support

If you found this project useful, please consider giving the repository a **⭐ Star**.

---

### 💡 Final Thought

This project demonstrates a practical approach to modern Data Engineering by treating each dataset according to its business behavior:

> **Increment where data grows.**
> **Overwrite where a complete refresh makes sense.**
> **Upsert where records change.**
> **Use SCD Type 2 where history matters.**
