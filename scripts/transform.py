# scripts/transform.py
import os, random

import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine


def transform_claims(df: pd.DataFrame) -> pd.DataFrame:
    """
    对 raw_claims 数据进行清洗与特征工程
    输入：从raw_claims 读取到的 DataFrame
    输出： 清洗 + 衍生字段后的 DataFrame
    """
    print("开始 Transform ...")

    df = df.copy()  # 保留原始数据
    # 如果 df 中没有 claim_date 才做兜底生成（防止首次运行问题）
    if 'claim_date' not in df.columns or df['claim_date'].isnull().all():
        print("警告：claim_date 缺失，生成模拟日期")
        base_date = datetime(2024, 1, 1)
        df["claim_date"] = [
            base_date + timedelta(days=random.randint(0, 730))
            for _ in range(len(df))
        ]

    # 1. 缺失值处理（现在没有，但生产中一定有)
    df['age'] = df['age'].fillna(df['age'].median())
    df['bmi'] = df['bmi'].fillna(df['bmi'].median())
    df['children'] = df['children'].fillna(0)
    df['charges'] = df['charges'].fillna(0)  # 或中位数，根据业务

    # 2. 类型确保
    df['age'] = df['age'].astype('int64')
    df['children'] = df['children'].astype('int64')
    df['charges'] = df['charges'].astype('float64')

    # 3. 日期处理
    df['claim_date'] = pd.to_datetime(df['claim_date'])
    df['claim_year'] = df['claim_date'].dt.year
    df['claim_month'] = df['claim_date'].dt.month
    df['claim_quarter'] = df['claim_date'].dt.quarter
    df['claim_day_of_week'] = df['claim_date'].dt.dayofweek  # 0=周一

    # 4. 衍生特征 -  常见理赔分析维度
    # 年龄分段
    bins_age = [0, 24, 34, 44, 54, 64, 120]
    labels_age = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']
    df['age_group'] = pd.cut(df['age'], bins=bins_age, labels=labels_age, right=False)

    # 理赔金额分组（关注高额理赔）
    bins_charge = [0, 5000, 15000, 30000, np.inf]
    labels_charge = ['Low (<5k)', 'Medium (5k-15k)', 'High (15k-30k)', 'Very High (>30k)']
    df['charge_group'] = pd.cut(df['charges'], bins=bins_charge, labels=labels_charge, right=False)

    # smoker -> 数值化（fraud相关强特征）
    df['smoker_flag'] = df['smoker'].map({'yes' : 1, 'no' : 0}).fillna(0).astype('int64')

    # 简单 fraud_flag 规则（生产中替换为ML模型）
    df['fraud_flag'] = (
        (df['charges'] > 30000) &
        (df['age'] < 30) &
        (df['smoker_flag'] == 1)
    ).astype('bool')

    # 5. 其他业务字段（加拿大保险常见）
    df['currency'] = 'CAD'  # 假设全部加拿大元
    df['claim_type'] = 'Medical'  # 数据集为医疗保险，可扩展

    # 6. 列顺序调整（更清晰）
    cols_order = [
        'id', 'age', 'age_group', 'sex', 'bmi', 'children', 'smoker', 'smoker_flag',
        'region', 'charges', 'charge_group', 'currency', 'claim_date',
        'claim_year', 'claim_month', 'claim_quarter', 'claim_day_of_week',
        'fraud_flag', 'claim_type'
    ]
    df = df[cols_order]

    print(f"Transform 完成，输出 {len(df)} 行，新增字段：{list(set(df.columns) - set(['id','age','sex','bmi','children','smoker','region','charges','claim_date','fraud_flag']))}")

    return df

def test_transform():
    """本地测试用：从数据库读取 → transform → 看结果"""

    load_dotenv()
    DB_USERNAME = "etl_user"
    DB_PASSWORD = "etl_pass123"
    DB_HOST = "postgres"
    DB_PORT = "5432"
    DB_NAME = "claims_db"
    DB_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DB_URL)

    df_raw = pd.read_sql("SELECT * FROM raw_claims ORDER BY id", engine)
    print(f"读取 raw_claims: {len(df_raw)} 行")

    df_transformed = transform_claims(df_raw)
    print("\nTransform 后前五行: ")
    print(df_transformed.head().to_string(index=False))

    # 看 fraud_flag 分布
    print("\nfraud_flag 分布：")
    print(df_transformed['fraud_flag'].value_counts())

    # 保存到本地
    ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    FILE_PATH = os.path.join(ROOT_DIR, "data", "processed", "transformed_sample.csv")
    df_transformed.to_csv(FILE_PATH, index=False)
    print(f"已保存样本到本地： {FILE_PATH}")

if __name__ == "__main__":
    test_transform()






    


