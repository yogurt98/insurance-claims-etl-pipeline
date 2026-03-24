# Insurance Claims ETL Pipeline
加拿大财产与意外险（P&C）理赔数据生产级 ETL 项目

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-orange.svg)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---
## Project Overview

This is a **production-grade ETL pipeline** for Canadian Property & Casualty (P&C) insurance claims processing. It demonstrates end-to-end data engineering skills suitable for roles at **Definity Financial**, **Sun Life**, or similar Canadian insurers.

The pipeline:
- Ingests raw insurance claims data (from Kaggle insurance dataset)
- Cleans and enriches data with business-relevant features (age groups, charge buckets, rule-based fraud flag)
- Orchestrates daily ETL via Apache Airflow
- Loads data to S3-compatible storage (LocalStack simulation) and a data warehouse (PostgreSQL simulating Redshift)
- Provides Power BI dashboard for claims trend, fraud analysis, and risk segmentation
---
## Key Features

- **Incremental Load** with PostgreSQL watermark + Airflow Variable control (`claims_full_refresh`)
- **Automated Data Quality Checks** (charges validation, age range, fraud rate) integrated in Airflow
- Full Airflow DAG with clear task dependencies: Extract → Transform → Load → DQ Check → Cache & Publish
- Feature engineering: `age_group`, `charge_group`, rule-based `fraud_flag`
- Event-driven architecture: publishes completion events to RabbitMQ
- Redis caching layer for fraud rules and latest summary
- Parquet intermediate format + Simulated AWS S3 (LocalStack) + Redshift (PostgreSQL)
- Power BI dashboard connected to `transformed_claims` table

---

## Tech Stack

- **Orchestration**: Apache Airflow 2.9 (LocalExecutor)
- **Language**: Python 3.11 + Pandas + SQLAlchemy
- **Database**: PostgreSQL 16 (source & simulated Redshift)
- **Storage**: LocalStack (AWS S3 simulation)
- **Messaging**: RabbitMQ
- **Cache**: Redis
- **Visualization**: Power BI Desktop
- **Others**: boto3, psycopg2, pika
---
## Project Structure

```bash
insurance-claims-etl-pipeline/
├── dags/
│   └── insurance_claims_etl_dag.py
├── scripts/
│   ├── extract.py              # Incremental + full refresh logic
│   ├── transform.py
│   ├── load_to_s3.py
│   ├── dq_check.py             # Data Quality validation
│   ├── queue_publish.py        # RabbitMQ event
│   ├── cache.py                # Redis cache
│   └── create_tables.py
├── models/
│   └── claims_models.py
├── data/
│   ├── raw/
│   └── processed/
├── powerbi/
│   └── claims_dashboard.pbix
├── docker-compose.yml
├── requirements.txt
└── README.md
```
## Architecture & Screenshots

### 1. Airflow DAG Overview
![Airflow DAG](screenshots/airflow_dag.png)

### 2. Incremental Load Control via Airflow Variables
![Airflow Variables](screenshots/airflow_variables.png)

### 3. Data Quality Check in Action
![DQ Check](screenshots/dq_check_success.png)

### 4. Power BI Dashboard
![Power BI Overview](screenshots/powerbi_dashboard.png)

### 5. RabbitMQ Event Publishing
![RabbitMQ](screenshots/rabbitmq_queues.png)
---

## How to Run (Quick Start)

1. Clone the repository

```bash
git clone https://github.com/yogurt98/insurance-claims-etl-pipeline.git
cd insurance-claims-etl-pipeline
```

2. Place Kaggle dataset
- Put insurance.csv into data/raw/insurance_claims_raw.csv

3. Start services
   ```bash
   docker compose up -d
4. Create database tablesBash
    ```bash
   docker compose exec airflow python scripts/create_tables.py

5. Configure run mode (in Airflow UI → Admin → Variables)
-   Set claims_full_refresh = True for first full load
-   Set to False for subsequent incremental loads

6. Open Airflow UI: http://localhost:8080 (admin / admin)
7. Enable & trigger the DAG: insurance_claims_etl_demo 
8. Verify:
-   PostgreSQL: transformed_claims table (~1338 rows)
-   S3 (LocalStack): s3://claims-bucket/transformed/claims.parquet
-   Local file: data/processed/transformed.parquet 
-   Power BI: connected to PostgreSQL
---
## Power BI Dashboard Highlights
Power BI Desktop is directly connected to the PostgreSQL transformed_claims table.
### Key Visuals:
- Line/Area chart: Sum of charges by claim month/quarter (trend analysis)
- Clustered column chart: Claims count / amount by age_group, colored by fraud_flag
- Pie/Donut chart: Proportion by charge_group (Low/Medium/High/Very High)
- Scatter plot: Age vs charges, colored by fraud_flag (risk pattern detection)
- Bar chart: Top regions by total claims amount
- KPI cards: Total claims count, total amount, average charge, fraud rate
- Slicers: age_group, region, fraud_flag, claim_month for interactive filtering

### Dashboard pages:

- Page 1: Overview & Trend
- Page 2: Risk & Fraud Analysis
- Page 3: Detail table (optional)

## Business Value for Canadian Insurers

- Production-ready patterns: Incremental loading, Data Quality enforcement, Event-driven architecture
- Fraud flag derivation with extensibility to ML models
- Simulated cloud environment (S3 + Redshift) aligning with Canadian insurance cloud migration trends
- End-to-end visibility from raw data to business insights via Power BI
- Fully Dockerized and easy to demonstrate in interviews
- 
## Future Improvements

- Incremental load optimization with proper late-arriving data handling
- Advanced Data Quality with Great Expectations
- Airflow CeleryExecutor for distributed execution
- Integration with dbt (see separate dbt-insurance-risk-warehouse repo)
- CI/CD pipeline and monitoring/alerting

## License
MIT
