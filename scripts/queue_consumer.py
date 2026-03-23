# scripts/queue_consumer.py
import time

import pika, os
import json
from datetime import datetime
from dotenv import load_dotenv
import redis

from scripts.rabbitmq_config import get_rabbitmq_params

load_dotenv()
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# ==============================
# 消费者逻辑 (手动或脚本运行)
# ==============================
def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        print(message)
        total_rows = message.get('total_rows', 0)
        fraud_count = message.get('fraud_count', 0)
        fraud_rate = (fraud_count / total_rows * 100) if total_rows > 0 else 0

        # 1. 更新 Redis summary（供下游快速查询）
        summary = {
            "total_rows": total_rows,
            "fraud_count": fraud_count,
            "fraud_rate_percent": round(fraud_rate, 2),
            "last_etl_time": message['timestamp'],
            "status": message['status'],
            # "message": message
        }

        redis_client.set("claims:summary:latest", json.dumps(summary))
        print(f"[Redis] 已更新最新理赔摘要: 欺诈率 {fraud_rate:.2f}%")

        # 2. 模拟业务动作：如果欺诈率 > 5%，发送“警报”
        if fraud_rate > 5:
            print("⚠️ 高欺诈率警报！建议立即人工审核最新批次理赔")
            # 实际可集成 Slack webhook / email / PagerDuty

        # 3. 可扩展：触发下游 ML 再评分
        # print("触发 ML 欺诈再评分任务...")

        print("[✅] 业务逻辑处理完成")
        ch.basic_ack(delivery_tag=method.delivery_tag)  # 手动确认
    except Exception as e:
        print(f" [ERROR] Failed to process: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) # 失败重入队列

def start_consumer():
    params = get_rabbitmq_params()
    print(f"[*] 启动消费者... 连接至 {params.host} (用户: {params.credentials.username})")

    try:
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue='claims_etl_completed', durable=True)
        channel.basic_qos(prefetch_count=1)  # 公平分发：一次只处理 1 条消息

        channel.basic_consume(
            queue='claims_etl_completed',
            on_message_callback=callback,
            auto_ack=False # 必须手动 ack
        )

        print('[*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] 消费者已停止")
    except Exception as e:
        print(f"❌ 消费者运行异常: {e}")
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()

# ==============================
# 入口点
# ==============================
if __name__ == "__main__":
    # 如果直接运行此脚本，默认启动消费者监听模式
    start_consumer()