# dags/insurance_claims_etl_dag.py
import os.path
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from dotenv import load_dotenv
from sqlalchemy import create_engine

# 导入我们写的脚本函数
from scripts.extract import extract_task
from scripts.load_to_s3 import create_s3_bucket, upload_to_s3, load_to_redshift_sim
from scripts.transform import transform_claims

# 注意：我们先不导入 load，等 DAG 能跑了再加

default_args = {
    'owner': 'jingxu',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='insurance_claims_etl_demo',
    default_args=default_args,
    description='加拿大保险公司理赔 ETL Demo - Extract + Transform',
    schedule_interval=None,  #  先手动触发，后面可以改成 '@daily'
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=['insurance', 'etl', 'claims', 'definity', 'sunlife'],
) as dag:

    @task
    def extract():
        """Task 1: 从 CSV 提取数据到 PostgreSQL raw_claims"""
        result = extract_task()
        return result

    @task
    def transform():
        """Task 2: 从 raw_claims 读取 → 清洗 + 衍生特征"""
        load_dotenv()
        DB_URL = f"postgresql://etl_user:etl_pass123@postgres:5432/claims_db"
        engine = create_engine(DB_URL)

        print("数据库连接字符串:", engine.url)
        df_raw = pd.read_sql("SELECT COUNT(*) AS cnt FROM raw_claims", engine)
        print("DAG 内读取 raw_claims 行数:", df_raw['cnt'].iloc[0])

        # 读取最新 raw 数据 （按 id 排序）
        df_raw = pd.read_sql("SELECT * FROM raw_claims ORDER BY id", engine)
        print(f"Transform 读取到 {len(df_raw)} 行数据")

        df_transformed = transform_claims(df_raw)

        # 暂时把 transformed 数据存成 Parquet 文件（后续 load 会用）
        ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        processed_path = os.path.join(ROOT_DIR, 'data', 'processed', 'transformed.parquet')
        df_transformed.to_parquet(processed_path, index=False, engine='pyarrow')
        print(f"已保存 transformed 数据到 {processed_path}")

        return {
            'transformed_rows': len(df_transformed),
            "parquet_path": processed_path
        }

    @task
    def load(transform_result):
        parquet_path = transform_result['parquet_path']

        # 创建 bucket （仅一次）
        create_s3_bucket()

        # 上传到 S3
        s3_url = upload_to_s3(parquet_path)

        # 模拟load 到 RedShift
        load_result = load_to_redshift_sim(parquet_path)

        return {
            's3_url': s3_url,
            **load_result
        }

    # DAG 流向：extract -> transform
    # extract_result = extract()
    extract()
    transform_result = transform()
    # load_result = load(transform_result)
    load(transform_result)



