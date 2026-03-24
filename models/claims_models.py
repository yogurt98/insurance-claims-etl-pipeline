# models/claims_models.py
# 本项目为了开发方便没有全都用导入为text，text的存储空间由于数据库算法优化对重复字符串大大压缩
# 相比于存储成本，生产崩溃导致的人工排查成本高得多，但在Mart层必须转为强类型以优化性能
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Numeric
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class RawClaim(Base):
    __tablename__ = "raw_claims"

    id = Column(Integer, primary_key=True, autoincrement=True)  # 数据上亿需要用BigInteger
    age = Column(Integer, nullable=True)                  # 客户年龄，nullable=True意味着要处理None
    sex = Column(String(10), nullable=True)               # 性别，10位string比CHAR(1)更有扩展性
    bmi = Column(Numeric, nullable=True)                    # 体质指数，要使用numeric，避免精度问题
    children = Column(Integer, nullable=True)             # 子女数
    smoker = Column(String(10), nullable=True)            # 是否吸烟（proxy fraud_flag）
    region = Column(String(50), nullable=True)            # 地区
    charges = Column(Numeric, nullable=True)              # 理赔金额（claim_amount）要使用numeric，避免精度问题
    claim_date = Column(DateTime, nullable=True, default=datetime.utcnow)  # 我们稍后会模拟日期
    fraud_flag = Column(Boolean, default=False)           # 衍生字段，后面 transform 里计算

    def __repr__(self):
        return f"<RawClaim(id={self.id}, age={self.age}, charges={self.charges}, smoker={self.smoker})>"

class TransformedClaim(Base):
    __tablename__ = "transformed_claims"

    id = Column(Integer, primary_key=True)
    age = Column(Integer)
    age_group = Column(String(10))
    sex = Column(String(10))
    bmi = Column(Float)
    children = Column(Integer)
    smoker = Column(String(10))
    smoker_flag = Column(Integer)
    region = Column(String(50))
    charges = Column(Float)
    charge_group = Column(String(20))
    currency = Column(String(10))
    claim_date = Column(DateTime)
    claim_year = Column(Integer)
    claim_month = Column(Integer)
    claim_quarter = Column(Integer)
    claim_day_of_week = Column(Integer)
    fraud_flag = Column(Boolean)
    claim_type = Column(String(50))


    def __repr__(self):
        return f"<TransformedClaim(id={self.id}, fraud_flag={self.fraud_flag})>"



class EtlWatermark(Base):
    __tablename__ = "etl_watermark"

    id = Column(Integer, primary_key=True)
    process_key = Column(String(100), unique=True, nullable=False)  # e.g. "claims_etl"
    last_processed_date = Column(DateTime, nullable=False, default=datetime(1900, 1, 1))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<EtlWatermark(key={self.process_key}, last_date={self.last_processed_date})>"