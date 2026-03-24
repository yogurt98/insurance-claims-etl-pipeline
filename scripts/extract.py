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
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")      # 容器内用 postgres
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
RAW_FILE = os.path.join(ROOT_DIR, "data", "raw", "insurance_claims_raw.csv")

PROCESS_KEY = "claims_main_etl"

# ── 增量管理辅助函数 ───────────────────────────────────────────
def get_last_processed_date(session) -> datetime:
    """获取上次处理的最大日期"""
    result = session.execute(
        text("SELECT last_processed_date FROM etl_watermark WHERE process_key = :key"),
        {"key": PROCESS_KEY}
    ).scalar()


    # 第一次运行，默认返回一个很早的日期（触发全量）
    return result if result else datetime(1900, 1, 1)


def update_watermark(session, new_max_date: datetime):
    """更新或插入 watermark"""
    session.execute(
        text("""
            INSERT INTO etl_watermark (process_key, last_processed_date, updated_at)
            VALUES (:key, :date, NOW())
            ON CONFLICT (process_key)
            DO UPDATE SET last_processed_date = :date, updated_at = NOW()
        """),
        {"key": PROCESS_KEY, "date": new_max_date}
    )
    session.commit()
    print(f"✅ Watermark 已更新: {new_max_date}")


# ── 核心逻辑 ──────────────────────────────────────────────────
def extract(full_refresh: bool = False) -> dict:
    """支持增量加载的主任务"""
    print("🚀 开始 Extract 任务...")

    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(f"原始文件不存在: {RAW_FILE}")

    session = SessionLocal()
    total_inserted = 0
    current_max_date = datetime(1900, 1, 1)

    try:
        # 获取上次处理时间
        last_processed = get_last_processed_date(session)
        # 🟢 核心修改点：如果外部强制 full_refresh，或者数据库没记录，才算全量
        is_actually_full = full_refresh or (last_processed.year == 1900)

        print(f"当前模式: {'全量加载 (首次运行)' if is_actually_full else '增量加载'}")


        if is_actually_full:
            print("🗑️ 模式: 全量加载 (正在清空 raw_claims 并重置 Watermark)")
            session.execute(text("TRUNCATE TABLE raw_claims RESTART IDENTITY;"))
            session.commit()
            last_processed = datetime(1900, 1, 1)

        print(f"上次处理日期: {last_processed}")

        # 2. 分块读取 CSV (防止内存爆炸)
        print(f"读取文件：{RAW_FILE}")
        print(f"📖 开始读取文件并分批处理 (每批 10000 条)...")
        reader = pd.read_csv(RAW_FILE, chunksize=10000)


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
            # 只在首次全量时生成 claim_date
            if 'claim_date' not in chunk.columns or chunk['claim_date'].isnull().all():
                base_date = datetime(2024, 1, 1)
                chunk["claim_date"] = [
                    base_date + pd.Timedelta(days=idx % 730) for idx in range(len(chunk))
                ]

            # fraud_flag 先全部设为 False，后面 transform 再判断
            chunk["fraud_flag"] = False

            # 增量过滤
            if not is_actually_full:
                chunk = chunk[chunk["claim_date"] > last_processed]

            if chunk.empty:
                continue

            # 记录当前批次的最大日期，用于更新 Watermark
            batch_max = chunk['claim_date'].max()
            if batch_max > current_max_date:
                current_max_date = batch_max

            # --- 批量插入数据库 ---
            records = [RawClaim(**row.to_dict()) for _, row in chunk.iterrows()]
            session.bulk_save_objects(records)
            session.commit()  # 每一批 commit 一次，释放内存

            total_inserted += len(records)
            print(f"   [Batch {i+1}] 成功插入 {len(records)} 条新记录")

        # 3. 更新 Airflow Watermark
        if total_inserted > 0:
            update_watermark(session, current_max_date)
        else:
            print("ℹ️  本次没有新数据，无需更新 Watermark")

        print(f"✅ Extract 完成，共处理 {total_inserted} 行")
        return {
            "extracted_rows": total_inserted,
            "is_first_run": is_actually_full,
            "mode": "full" if is_actually_full else "incremental"
        }

    except Exception as e:
        session.rollback()
        print(f"❌ Extract failed: {str(e)}")
        raise
    finally:
        session.close()


# ── Airflow Task 接口


def extract_task(full_refresh: bool = False):
    print("开始 Extract 任务")
    result = extract(full_refresh=full_refresh)
    print(f"Extract 任务完成， 处理{result['extracted_rows']}行")
    return result


if __name__ == "__main__":
    print("开始执行 extract → PostgreSQL")
    extract()
    print("执行结束")