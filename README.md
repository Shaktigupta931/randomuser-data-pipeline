# RandomUser Data Engineering Pipeline

## 📌 Project Overview

This project implements an end-to-end data pipeline that:

- Ingests user data from the RandomUser API
- Normalizes nested JSON into relational tables
- Loads data into Google BigQuery
- Ensures idempotent re-runs using MERGE logic
- Implements retry handling
- Tracks pipeline execution using system audit tables

The pipeline is built using Python and Google BigQuery.

---

## 🏗 Architecture

```
RandomUser API
        ↓
API Ingestion (Retry Enabled)
        ↓
Transformation (JSON → Normalized Tables)
        ↓
BigQuery Load
    - Staging Table
    - MERGE into Target Tables
        ↓
System Tables (Audit + Metrics)
```

---

## 📂 Project Structure

```
randomuser_pipeline/
│
├── config/
│   └── config.yaml
│
├── credentials/
│   └── service_account.json
│
├── src/
│   ├── ingestion/
│   │   └── api_client.py
│   ├── transform/
│   │   └── transformer.py
│   ├── load/
│   │   └── bigquery_loader.py
│   ├── system/
│   │   └── system_tables.py
│   ├── utils/
│   │   └── helpers.py
│   └── main.py
│
├── requirements.txt
└── README.md
```

---

## 🔐 Authentication Handling

- Uses Google Service Account JSON key
- Authentication handled via:

```python
service_account.Credentials.from_service_account_file(...)
```

- Access tokens are automatically refreshed by Google SDK
- No manual token handling required

---

## 🌐 API Ingestion

API Used:
```
https://randomuser.me/api/?results=100&seed=easytest
```

Features:
- Retry handling with exponential backoff
- Network failure handling
- 5xx response handling

---

## 🧩 Schema Design (Normalization)

The API returns deeply nested JSON.  
The schema was normalized into 7 relational tables.

### Core Tables

#### 1️⃣ users
- user_id (Primary Key)
- gender
- email
- phone
- cell
- nat
- registered_date

#### 2️⃣ names
- user_id (Foreign Key)
- title
- first
- last

#### 3️⃣ locations
- user_id
- street_number
- street_name
- city
- state
- country
- postcode
- latitude
- longitude
- timezone_offset
- timezone_description

#### 4️⃣ logins
- user_id
- username
- password
- salt
- md5
- sha1
- sha256

#### 5️⃣ dobs
- user_id
- dob
- age

#### 6️⃣ ids
- user_id
- id_name
- id_value

#### 7️⃣ pictures
- user_id
- large
- medium
- thumbnail

### Primary Key Strategy

The `login.uuid` field from the API is used as the primary key (`user_id`) across all tables to maintain referential integrity.

---

## 🔄 Idempotency Strategy

To ensure safe re-runs:

1. Data is loaded into a staging table
2. A BigQuery MERGE statement is executed:

```sql
MERGE target T
USING staging S
ON T.user_id = S.user_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

This guarantees:

- No duplicate records
- Safe pipeline re-execution
- Data consistency

---

## 📊 System Tables

Three system tables were created:

### metadata_info
Tracks pipeline execution metadata:
- run_id
- dataset_name
- run_timestamp
- status
- records_fetched

### pipeline_audit_logs
Tracks step-level logging:
- run_id
- step_name
- status
- message
- timestamp

### pipeline_metrics
Tracks execution metrics:
- run_id
- records_loaded
- timestamp

System tables use APPEND logic to preserve historical runs.

---

## 📈 Logging Strategy

The pipeline logs:

- Pipeline start and completion
- API record count
- Per-table merge row counts
- Execution time
- Errors (if any)

Row counts are logged during each MERGE operation to validate idempotency.

---

## 🚀 How to Run Locally

### 1️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 2️⃣ Place Service Account File

Put the Service Account JSON inside:

```
credentials/service_account.json
```

### 3️⃣ Run the Pipeline

From project root:

```bash
python3 -m src.main
```

---

## ☁️ Automation Strategy

The pipeline can be automated using any cloud platform. Below are recommended approaches:

### Option 1: Cloud Run + Cloud Scheduler (Recommended)

1. Containerize the application using Docker
2. Push the image to Google Artifact Registry
3. Deploy the container to Cloud Run
4. Use Cloud Scheduler to trigger execution on a schedule

Benefits:
- Serverless
- Scalable
- Fully managed
- Minimal operational overhead

---

### Option 2: GitHub Actions + GCP

1. Store repository in GitHub
2. Create workflow YAML
3. Authenticate using service account secret
4. Trigger pipeline on schedule or push

Benefits:
- CI/CD integration
- Version control
- Automated deployments

---

## 🎯 Design Highlights

- Modular architecture
- Config-driven pipeline
- Service account authentication
- Retry handling
- Idempotent MERGE loading
- Normalized relational schema
- System audit and metrics tracking
- Automation-ready design

---

## ✅ Assessment Completion

✔ API ingestion  
✔ 7 normalized tables  
✔ 3 system tables  
✔ BigQuery loading  
✔ Retry handling  
✔ Idempotency  
✔ Service account authentication  
✔ Automation explanation  

---

## 📌 Conclusion

This project demonstrates production-grade data engineering practices including:

- Clean modular architecture
- Idempotent data loading
- Observability
- Secure authentication
- Scalable automation design
