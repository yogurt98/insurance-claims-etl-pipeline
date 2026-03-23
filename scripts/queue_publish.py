# scripts/queue_publish.py
import time

import pika, os
import json
from datetime import datetime
from dotenv import load_dotenv

from scripts.rabbitmq_config import get_rabbitmq_params

load_dotenv()



# 生产者逻辑 (由 Airflow 调用)
def publish_etl_completed_event(rows: int, fraud_count: int):
    params = get_rabbitmq_params()
    print(f"DEBUG: 正在发布事件... 用户名: {params.credentials.username}")
    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        # 声明队列（确保队列存在，durable=True 消息持久化）
        channel.queue_declare(queue='claims_etl_completed', durable=True)

        message = {
            "event": "claims_etl_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "total_rows": rows,
            "fraud_count": fraud_count,
            "status": "success"
        }

        # 发布消息
        channel.basic_publish(
            exchange='',
            routing_key='claims_etl_completed',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # 持久化
        )
        connection.close()
        print(f"✅ RabbitMQ 事件已发布：处理 {rows} 条，欺诈 {fraud_count} 条")
    except Exception as e:
        print(f"❌ RabbitMQ publish failed: {str(e)}")
        # 抛出异常让 Airflow 捕获并记录失败状态
        raise e

