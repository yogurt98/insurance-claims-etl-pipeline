# scripts/load_to_s3.py
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# LocalStack S3 配置 （模拟AWS）
S3_ENDPOINT = 'http://localstack:4566'
AWS_ACCESS_KEY = 'test'
AWS_SECRET_KEY = 'test'
BUCKET_NAME = 'claims-bucket'

# PostgreSQL （模拟 Redshift）
DB_URL = "postgresql://etl_user:etl_pass123@postgres:5432/claims_db"
engine = create_engine(DB_URL)

def create_s3_bucket():
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"S3 Bucket '{BUCKET_NAME}' 创建成功（或已存在）")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print("Bucket 已存在")
        else:
            raise

def upload_to_s3(parquet_path: str, s3_key: str = 'transformed/claims.parquet'):
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    try:
        s3_client.upload_file(
            parquet_path,
            BUCKET_NAME,
            s3_key
        )
        print(f"上传成功：s3://{BUCKET_NAME}/{s3_key}")
        return f"s3://{BUCKET_NAME}/{s3_key}"
    except Exception as e:
        print(f"上传失败：{str(e)}")
        raise

def load_to_redshift_sim(parquet_path: str):
    # 读取 parquet → 插入 PostgreSQL transformed_claims
    df = pd.read_parquet(parquet_path)
    print(f"准备加载 {len(df)} 行到 transformed_claims")

    # 全量覆盖
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE transformed_claims RESTART IDENTITY;"))

    # 插入（用 pandas to_sql，生产中可用 COPY FROM）
    df.to_sql(
        'transformed_claims',
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    print("加载到 transformed_claims 完成")

    return {"loaded_rows": len(df)}




