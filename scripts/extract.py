# scripts/extract.py
import os, random
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.claims_models import RawClaim, Base
from datetime import datetime, timedelta
from dotenv import load_dotenv

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
ROOT_DIR =  os.path.dirname(os.path.dirname((os.path.abspath(__file__))))

RAW_FILE = os.path.join(ROOT_DIR, "data", "raw", "insurance_claims_raw.csv")   # ← 改成你实际的文件名

def extract_and_load_raw(full_refresh: bool = True):
    if not os.path.exists(RAW_FILE):
        print(f"文件不存在：{RAW_FILE}")
        return

    print(f"读取文件：{RAW_FILE}")
    df = pd.read_csv(RAW_FILE)

    # 字段映射（kaggle insurance.csv 的标准字段）
    # 前者为csv，后者为数据库
    # 我们做最小的转换，保持接近原始数据
    # 在大型项目中，建议在 rename 后加一个校验：
    # assert set(rename_map.values()).issubset(df.columns)
    rename_map = {
        "age": "age",
        "sex": "sex",
        "bmi": "bmi",
        "children": "children",
        "smoker": "smoker",
        "region": "region",
        "charges": "charges",
    }
    df = df.rename(columns=rename_map)

    # 类型处理（让 SQLAlchemy 能顺利插入）
    df["age"] = pd.to_numeric(df["age"], errors='coerce').astype("Int64")
    df["bmi"] = pd.to_numeric(df["bmi"], errors="coerce")
    df["children"] = pd.to_numeric(df["children"], errors="coerce").astype("Int64")
    df["charges"] = pd.to_numeric(df["charges"], errors="coerce")

    # # 模拟 claim_date（因为原数据集没有，我们用当前时间填充）
    # df["claim_date"] = datetime.utcnow()

    # 模拟真实理赔日期：随机分布在过去 2 年内
    base_date = datetime(2024, 1, 1)
    df["claim_date"] = [
        base_date + timedelta(days=random.randint(0, 730))  # 随机 0~730 天
        for _ in range(len(df))
    ]


    # fraud_flag 先全部设为 False，后面 transform 再判断
    df["fraud_flag"] = False

    print(f"读取到{len(df)}行数据")

    # ── 插入数据库（幂等方式） ────────────────────────────────
    session = SessionLocal()
    inserted = 0

    try:
        # 先检查表里已有多少记录（用于对比）
        if full_refresh:
            # 全量刷新（Full Refresh）
            session.execute(text("TRUNCATE TABLE raw_claims RESTART IDENTITY;"))
            session.commit()
            print("已清空 raw_claims 表（full refresh 模式）")

        count_before = session.execute(text("SELECT COUNT(*) FROM raw_claims")).scalar()
        print(f"插入前有{count_before}条记录")


        # 批量插入（效率更高，但不适用10000条以上，会爆内存）

        records = [RawClaim(**row.to_dict()) for _ , row in df.iterrows()]
        session.bulk_save_objects(records)
        session.commit()
        inserted = len(records)
        print(f"成功插入{inserted} 条记录")

        count_after = session.execute(text("SELECT COUNT(*) FROM raw_claims")).scalar()
        print(f"插入后表中有 {count_after} 条记录")

    except Exception as e:
        session.rollback()
        print(f"❌ 插入失败: {str(e)}")
        raise

    finally:
        session.close()

    return inserted


if __name__ == "__main__":
    print("开始执行 extract → PostgreSQL")
    extract_and_load_raw(full_refresh=True)
    print("执行结束")

def extract_task():
    print("开始 Extract 任务")
    rows = extract_and_load_raw(full_refresh = True)
    print(f"Extract 任务完成， 处理{rows}行")
    return {"extracted_rows": rows}