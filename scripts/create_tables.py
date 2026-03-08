# scripts/create_tables.py
import os

from sqlalchemy.future import create_engine

from models.claims_models import Base

# 读取环境变量（或写死也可以，方便本地调试）
DB_USER = os.getenv("POSTGRES_USER", "etl_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "etl_pass123")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")          # docker 网络里用服务名
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "claims_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

print("正在连接数据库并创建表...")
Base.metadata.create_all(engine)
print("表创建完成！表名：raw_claims")