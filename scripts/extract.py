import logging
import os
from typing import Generator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.claims_models import RawClaim, Base
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── 配置与日志 ────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
    # 无论日志级别是否是 DEBUG，字符串拼接都会立即发生
    # logging.debug(f"处理数据: {massive_data_object}")
    # 在高并发场景下，数百万次无效的 f-string 拼接会累积显著的 CPU 开销。
    # 使用占位符风格，日志平台可以更轻松地将消息模板（Message Template）与具体变量（Arguments）分离，
    # 方便进行日志聚合（Log Aggregation）。
    # 因此选择用"%()s"

logger = logging.getLogger("ETL-Loader")

class RawDataLoader:
    def __init__(self):
        self.db_url = self._get_db_url()
        self.engine = create_engine(
            self.db_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True  # 检查链接有效性
        )
        self.SessionLocal = sessionmaker(bind=self.engine)
        current_script_path = os.path.abspath(__file__)  # 获取当前脚本绝对路径
        self.project_root = os.path.dirname(os.path.dirname(current_script_path)) # 获取项目根目录
        # defalt_raw_file_path = os.path.join(self.project_root, "data/raw/insurance_claims_raw.csv" )
        # 用下面的写法，避免用windows运行的时候不能用
        defalt_raw_file_path = os.path.join(self.project_root, "data", "raw", "insurance_claims_raw.csv")
        self.raw_file_path = os.getenv("RAW_FILE_PATH", defalt_raw_file_path)
        self.chunk_size = 10000 # 分批处理，每次只读取并处理一万行，处理完即释放，避免爆内存

    def _get_db_url(self) -> str:
        # 策略：核心凭据用 environ（强制检查，空值直接抛出 KeyError），默认配置用 getenv（灵活宽容，空值返回 None 或默认值）
        user = os.getenv("POSTGRES_USER", "etl_user")       # 可选配置，允许 fallback，返回默认值“etl_user”
        # password = os.environ["POSTGRES_PASSWORD"]          # 必填配置，遵循“早崩溃早修复”原则
        password = os.getenv("POSTGRES_PASSWORD", "etl_pass123")  # 暂时在开发中用这个获取返回值
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        db = os.getenv("POSTGRES_DB", "claims_db")
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        高效数据清洗与预处理（工业级重构版）
        - 采用向量化映射减少内存拷贝
        - 强制对齐 SQLAlchemy 模型中的 Numeric 和 Boolean 类型
        - 增加鲁棒的空值过滤
        """
        if df.empty:
            return df
        # 1. 定义字段映射表(Schema)
        # 使用Int64（nullable integer）和float64
        type_mapping = {
            "age": "Int64",
            "children": "Int64",
            "bmi": "float64",
            "charges": "float64"
        }

        # 2. 批量化处理数值转换（向量化操作）
        # pd.to_numeric 在处理整表的时候非常快
        # 先强制转为数值（处理脏字符串,空值转为NaN），再统一强制转换类型（实现 Int64 的可空逻辑),避免多次内存分配
        cols_to_convert = list(type_mapping.keys())
        df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors="coerce").astype(type_mapping)

        # 3. 批量增加审计字段
        # 避免循环赋值
        now = datetime.now(timezone.utc)
        df = df.assign(
            claim_date=now,
            fraud_flag=False
        )
        # 5. 高效清洗 (In-place filter)
        # 工业级数据量大时，通过布尔索引直接过滤
        # 注意：这里会返回一个 View 或新 DF，取决于内存状态
        # 但有时候我们不能删掉数据，这可能会丢失信息
        df = df.dropna(subset=['age', 'charges'])

        return df

    def get_data_chunks(self) -> Generator[pd.DataFrame, None, None]:
        """利用 pandas chunksize 读取大数据集， 避免 OOM （内存溢出）"""
        if not os.path.exists(self.raw_file_path):
            raise FileNotFoundError(f"Source file not found: {self.raw_file_path}")

        logging.info(f"🚀 Starting stream read from {self.raw_file_path}")
        # 使用 chunksize 返回迭代器
        return pd.read_csv(self.raw_file_path, chunksize=self.chunk_size)

    def load_to_db(self):
        """核心加载逻辑：批量插入 + 事务控制"""
        chunks = self.get_data_chunks()
        total_inserted = 0

        with self.SessionLocal() as session:
            try:
                for i, df_chunk in enumerate(chunks):
                    # 数据预处理
                    processed_df = self.process_dataframe(df_chunk)

                    # 转换为模型字典列表，用于批量插入
                    records = processed_df.to_dict(orient="records")

                    # 工业级写法：使用 bulk_insert_mappings 绕过对象实例化开销
                    session.bulk_insert_mappings(RawClaim, records)

                    # 分批提交事务，防止长事务锁表或 Undo Log 撑爆
                    session.commit()

                    batch_count = len(processed_df)
                    total_inserted += batch_count
                    logger.info(f"✅ Batch {i+1} committed: {batch_count} rows. Total: {total_inserted}")

                logger.info("🎯 ETL Load Job Completed Successfully.")

            except Exception as e:
                session.rollback()
                logger.error(f"❌ Transaction failed, rolled back. Error: {str(e)}")
                raise e

if __name__ == "__main__":
    loader = RawDataLoader()

    logger.info("正在检查并创建表结构...")
    Base.metadata.create_all(bind=loader.engine)

    loader.load_to_db()







