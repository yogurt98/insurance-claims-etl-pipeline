# scripts/cache.py
import redis
import json
from datetime import datetime

def cache_fraud_rules_and_summary():
    r = redis.Redis(host='redis', port=6379, db=0)

    # 缓存欺诈规则（生产中可动态更新）
    rules = {
        "high_risk_age": [18, 30],
        "high_risk_charge_threshold": 30000,
        "last_updated": datetime.utcnow().isoformat()
    }
    r.set("fraud:rules", json.dumps(rules))

    # 缓存最新 summary（供 Power BI 或下游快速读取）
    summary = {"total_claims": 1338, "fraud_count": 46, "total_amount": 13270.42}
    r.set("claims:summary:latest", json.dumps(summary))

    print("Redis 缓存更新成功（欺诈规则 + 最新摘要）")
    return True