# scripts/validate_raw_data.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "etl_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "etl_pass123")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")  # 容器内用 "postgres"
DB_PORT = "5432"
DB_NAME = "claims_db"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

def validate_raw_claims():
    print("=== 开始验证 raw_claims 表 ===")

    # 总行数
    df_count = pd.read_sql("SELECT COUNT(*) AS cnt FROM raw_claims", engine)
    total_rows = df_count['cnt'].iloc[0]  # 也可以用.iat[0], session.execute(...).scalar()
    print(f"总行数：{total_rows}")

    if total_rows == 0:
        print("警告：列表为空！")
        return

    # 显示前5行
    print("\n前五行样例")
    df_sample = pd.read_sql("SELECT * FROM raw_claims LIMIT 5", engine)
    print(df_sample.to_string(index=False))

    # 基本统计
    stats_query = """
    SELECT 
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE age IS NULL) AS null_age,
        MIN(age) AS min_age, AVG(age) AS avg_age, MAX(age) AS max_age,
        COUNT(*) FILTER (WHERE charges <= 0) AS non_positive_charge,
        MIN(charges) AS min_charge, AVG(charges)::numeric(12,2) AS avg_charge, MAX(charges) AS max_charge,
        COUNT(DISTINCT region) AS unique_regions,
        SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) AS fraud_count
    FROM raw_claims
    """
    df_stats = pd.read_sql(stats_query, engine)
    print()
    print("\n基本统计：")
    print(df_stats.to_string(index=False))

    # 简单质量检查
    issues = []
    if df_stats['null_age'].iloc[0] > 0:
        issues.append(f"有 {df_stats['null_age'].iloc[0]} 个age 为null")
    if df_stats['min_age'].iloc[0] < 0 or df_stats['max_age'].iloc[0] > 120:
        issues.append("age 有异常值（ <0 或 >120）")
    if df_stats['non_positive_charge'].iloc[0] > 0:
        issues.append(f"有 {df_stats['non_positive_charge'].iloc[0]} 条 charges <= 0")
    if df_stats['fraud_count'].iloc[0] > 0:
        issues.append("fraud_flag 已有 true 值（可能之前 transform 跑过）")

    if issues:
        print("\n发现问题：")
        for issue in issues:
            print(f"- {issue}")
    else:
        print("\n数据质量初步检查：无明显严重问题")

    print("=== 验证结束 ===")


if __name__ == "__main__":
    validate_raw_claims()
