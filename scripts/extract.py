# scripts/extract.py
import os, random
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.claims_models import RawClaim, Base
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow.models import Variable

load_dotenv()

# ── 数据库连接 ────────────────────────────────────────────────
DB_USER = os.getenv("POSTGRES_USER", "etl_user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "etl_pass123")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")      # 本地调试用 localhost，容器内用 postgres
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "claims_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# 每次session结束一定要关闭，不然会耗尽内存资源。session.commit() -> session.clear()
# 每10000条（一个chunk）执行一遍，避免爆内存
# 可以不用session只用engine，但只能获得原始tuple且会让代码里充满sql语句

# ── 文件路径 ──────────────────────────────────────────────────
ROOT_DIR = os.path.dirname(os.path.dirname((os.path.abspath(__file__))))
RAW_FILE = os.path.join(ROOT_DIR, "data", "raw", "insurance_claims_raw.csv")   # ← 改成你实际的文件名

# ── 增量管理辅助函数 ───────────────────────────────────────────
def get_last_processed_date():
    """获取上次处理的最大日期"""
    var_key = "claims_last_processed_date"
    # 如果 Variable 不存在，默认从 1900 年开始（全量）
    last_date_str = Variable.get(var_key, default_var="1900-01-01")
    return datetime.fromisoformat(last_date_str)

def update_last_processed_date(new_max_date: datetime):
    """更新 Airflow 变量"""
    var_key = "claims_last_processed_date"
    # 转换成字符串存储
    Variable.set(var_key, new_max_date.isoformat())
    print(f"✅ 更新 Airflow 变量 {var_key} 为 {new_max_date}")


# ── 核心逻辑 ──────────────────────────────────────────────────
def extract():
    full_refresh = Variable.get("claims_full_refresh", default_var="False") == "True"
    incremental = not full_refresh
    last_date = get_last_processed_date() if incremental else datetime(1900, 1, 1)

    print(f"开始 Extract 任务 | Full Refresh: {full_refresh} | Incremental: {incremental} | Start Date: {last_date}")
    if not os.path.exists(RAW_FILE):
        print(f"文件不存在：{RAW_FILE}")
        return

    session = SessionLocal()
    total_inserted = 0
    new_watermark = last_date # 初始设为旧的，后面对比更新

    try:
        # 1. 如果是全量刷新，先清空表
        if full_refresh:
            print("🗑️ Full Refresh 模式：正在清空 raw_claims 表...")
            session.execute(text("TRUNCATE TABLE raw_claims RESTART IDENTITY;"))
            session.commit()

        # 2. 分块读取 CSV (防止内存爆炸)
        print(f"读取文件：{RAW_FILE}")
        print(f"📖 开始读取文件并分批处理 (每批 10000 条)...")
        reader = pd.read_csv(RAW_FILE, chunksize=10000)

        # 记录已读取的总行数，用于生成确定性的日期
        global_row_count = 0

        for i, chunk in enumerate(reader):
            # --- 数据清洗与模拟日期 ---
            # 字段映射（kaggle insurance.csv 的标准字段）
            # 前者为csv，后者为数据库
            # 我们做最小的转换，保持接近原始数据
            rename_map = {"age": "age", "sex": "sex", "bmi": "bmi", "children": "children",
                          "smoker": "smoker", "region": "region", "charges": "charges"}
            chunk = chunk.rename(columns=rename_map)

            # 类型处理（让 SQLAlchemy 能顺利插入）
            chunk["age"] = pd.to_numeric(chunk["age"], errors='coerce').astype("Int64")
            chunk["bmi"] = pd.to_numeric(chunk["bmi"], errors="coerce")
            chunk["children"] = pd.to_numeric(chunk["children"], errors="coerce").astype("Int64")
            chunk["charges"] = pd.to_numeric(chunk["charges"], errors="coerce")


            # 模拟真实理赔日期：随机分布在过去 2 年内，增量时不重新生成
            if full_refresh or chunk.get('claim_date') is None:
                base_date = datetime(2024, 1, 1)
                chunk["claim_date"] = [
                    base_date + timedelta(days=idx % 730)  # 固定循环 2 年
                    for idx in range(len(chunk))
                ]


            # fraud_flag 先全部设为 False，后面 transform 再判断
            chunk["fraud_flag"] = False

            # --- 核心增量过滤 ---
            if incremental and not full_refresh:
                chunk = chunk[chunk['claim_date'] > last_date]

            if chunk.empty:
                continue

            # 记录当前批次的最大日期，用于更新 Watermark
            batch_max = chunk['claim_date'].max()
            if batch_max > new_watermark:
                new_watermark = batch_max

            # --- 批量插入数据库 ---
            records = [RawClaim(**row.to_dict()) for _, row in chunk.iterrows()]
            session.bulk_save_objects(records)
            session.commit()  # 每一批 commit 一次，释放内存

            total_inserted += len(records)
            print(f"   [Batch {i+1}] 成功插入 {len(records)} 条新记录")

        # 3. 更新 Airflow Watermark
        if incremental and total_inserted > 0:
            update_last_processed_date(new_watermark)
        print(f"Extract 完成，共处理 {total_inserted} 行")
        return {"extracted_rows": total_inserted, "full_refresh": full_refresh}

    except Exception as e:
        session.rollback()
        print(f"❌ ETL 过程出错: {str(e)}")
        raise
    finally:
        session.close()






# ── Airflow Task 接口
def extract_task():
    print("开始 Extract 任务")
    result = extract()
    print(f"Extract 任务完成， 处理{result['extracted_rows']}行")
    return result

if __name__ == "__main__":
    print("开始执行 extract → PostgreSQL")
    extract()
    print("执行结束")