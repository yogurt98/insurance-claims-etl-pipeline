# scripts/dq_check.py
from sqlalchemy import text
from airflow.exceptions import AirflowException
from dotenv import load_dotenv
import os

load_dotenv()

# 使用和 extract 相同的数据库连接
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER', 'etl_user')}:{os.getenv('POSTGRES_PASSWORD', 'etl_pass123')}@{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'claims_db')}"

def run_dq_checks():
    print("=== 开始执行 Data Quality Check ===")

    checks = [
        {"name": "charges_not_null_or_negative",
         "query": "SELECT COUNT(*) FROM transformed_claims WHERE charges IS NULL OR charges <= 0",
         "threshold": 0},

        {"name": "age_valid_range",
         "query": "SELECT COUNT(*) FROM transformed_claims WHERE age < 18 OR age > 120",
         "threshold": 0},

        {"name": "fraud_rate_reasonable",
         "query": """
            SELECT 
                CASE WHEN COUNT(*) = 0 THEN 0 
                     ELSE ROUND(SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END)::numeric * 100 / COUNT(*), 2)
                END 
            FROM transformed_claims
         """,
         "threshold": 15.0},   # 欺诈率不超过 15%
    ]

    from sqlalchemy import create_engine
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        for check in checks:
            result = conn.execute(text(check["query"])).scalar()
            print(f"{check['name']}: {result}")

            if result > check["threshold"]:
                error_msg = f"DQ Check 失败！{check['name']} = {result} > 阈值 {check['threshold']}"
                print(f"❌ {error_msg}")
                raise AirflowException(error_msg)

    print("✅ 所有 Data Quality Check 通过！")
    return {"dq_status": "passed"}