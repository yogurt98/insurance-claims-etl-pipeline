# scripts/rabbitmq_config.py
import pika, os
from dotenv import load_dotenv
load_dotenv()

def get_rabbitmq_params():
    # 统一从环境变量读取，避免两个文件写得不一样
    user = os.getenv('AIRFLOW__RABBITMQ_USER', 'admin')
    password = os.getenv('AIRFLOW__RABBITMQ_PASS', 'admin123')
    host = os.getenv('RABBITMQ_HOST', 'rabbitmq')

    credentials = pika.PlainCredentials(user, password)
    return pika.ConnectionParameters(
        host=host,
        port=5672,
        credentials=credentials,
        heartbeat=600
    )