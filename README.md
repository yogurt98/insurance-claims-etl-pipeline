# Insurance Claims ETL Pipeline
加拿大财产与意外险（P&C）理赔数据生产级 ETL 项目

English 

## Project Overview

This is a **production-grade ETL pipeline** for Canadian Property & Casualty (P&C) insurance claims processing. It demonstrates end-to-end data engineering skills suitable for roles at **Definity Financial**, **Sun Life**, or similar Canadian insurers.

The pipeline:
- Ingests raw insurance claims data (from Kaggle insurance dataset)
- Cleans and enriches data with business-relevant features (age groups, charge buckets, rule-based fraud flag)
- Orchestrates daily ETL via Apache Airflow
- Loads data to S3-compatible storage (LocalStack simulation) and a data warehouse (PostgreSQL simulating Redshift)
- Provides Power BI dashboard for claims trend, fraud analysis, and risk segmentation

## Key Features

- Full Airflow DAG with Extract → Transform → Load tasks
- Bulk insert & full refresh pattern (common in insurance batch processing)
- Feature engineering: age_group, charge_group, fraud_flag (rule-based, extensible to ML)
- Parquet intermediate format for efficient storage & transfer
- Simulated AWS S3 (LocalStack) + Redshift (PostgreSQL)
- Power BI visuals for business insights (claims trend, age/risk distribution, fraud rate)

## Tech Stack

- Python 3.11 + Pandas + SQLAlchemy
- Apache Airflow 2.9 (LocalExecutor)
- PostgreSQL 16 (source & simulated Redshift)
- LocalStack (AWS S3 simulation)
- boto3 (S3 upload)
- Power BI Desktop (dashboard)

## Project Structure

insurance-claims-etl-pipeline/
├── dags/
│   └── insurance_claims_etl_dag.py
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── create_tables.py
├── models/
│   └── claims_models.py
├── data/
│   ├── raw/                     # insurance_claims_raw.csv
│   └── processed/               # transformed.parquet
├── powerbi/                     # claims_dashboard.pbix (optional)
├── docker-compose.yml
├── requirements.txt
└── README.md


## How to Run (Quick Start)

1. Clone the repo
2. Place Kaggle `insurance.csv` into `data/raw/insurance_claims_raw.csv`
3. Start services
   ```bash
   docker compose up -d
4. Create database tablesBash
    ```bash
   docker compose exec airflow python scripts/create_tables.py
5. Open Airflow UI: http://localhost:8080 (admin / admin)
6. Enable & trigger the DAG: insurance_claims_etl_demo 
7. Verify:
   PostgreSQL: transformed_claims table (~1338 rows)
   S3 (LocalStack): s3://claims-bucket/transformed/claims.parquet
   Local file: data/processed/transformed.parquet

## Power BI Dashboard Highlights
Connected to PostgreSQL transformed_claims table.
Recommended visuals (already implemented or easy to add):

Line/Area chart: Sum of charges by claim month/quarter (trend analysis)
Clustered column chart: Claims count / amount by age_group, colored by fraud_flag
Pie/Donut chart: Proportion by charge_group (Low/Medium/High/Very High)
Scatter plot: Age vs charges, colored by fraud_flag (risk pattern detection)
Bar chart: Top regions by total claims amount
KPI cards: Total claims count, total amount, average charge, fraud rate
Slicers: age_group, region, fraud_flag, claim_month for interactive filtering

Dashboard pages:

Page 1: Overview & Trend
Page 2: Risk & Fraud Analysis
Page 3: Detail table (optional)

## Business Value for Canadian Insurers

End-to-end orchestration with Airflow (daily batch processing common in P&C)
Fraud flag derivation (rule-based foundation for future ML models)
S3 + Redshift-like pattern (aligns with cloud migration in Canadian insurance)
Power BI integration shows business impact (claims monitoring, risk segmentation)
Dockerized & demo-friendly for interviews

## Future Improvements

Incremental load (using delta detection or watermark)
ML-based fraud scoring (integrate scikit-learn or simple model)
dbt for downstream modeling
Monitoring & alerting (Airflow sensors + Slack/Email)
CI/CD pipeline (GitHub Actions)

## License
MIT