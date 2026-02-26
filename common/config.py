"""
配置文件
现在优先从 .env 文件加载配置，未设置时使用默认值
"""

import os
from dotenv import load_dotenv

load_dotenv()

# API配置（兼容保留）
BASE_URL = os.getenv('BASE_URL', "https://gateway.test.nado.xyz/v1")

# 交易所配置
EXCHANGE_TYPE = os.getenv('EXCHANGE_TYPE', 'nado')

# 兼容保留字段（Nado 不使用）
L1_ADDRESS = os.getenv('L1_ADDRESS', '')
ACCOUNT_INDEX = int(os.getenv('ACCOUNT_INDEX', '0'))
API_KEY_INDEX = int(os.getenv('API_KEY_INDEX', '0'))
API_KEY_PRIVATE_KEY = os.getenv('API_KEY_PRIVATE_KEY', '')

# 日志配置
LOG_LEVEL = os.getenv('LOG_LEVEL', "INFO")
LOG_FORMAT = os.getenv('LOG_FORMAT', "")

# 代理配置
PROXY_URL = os.getenv('PROXY_URL', '')

# PostgreSQL 配置
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'yourpassword')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'mydb')

# 钉钉通知配置（webhook 为空时关闭通知）
DINGTALK_WEBHOOK = os.getenv('DINGTALK_WEBHOOK', '')  # 完整的钉钉机器人 Webhook 地址
DINGTALK_KEYWORD = os.getenv('DINGTALK_KEYWORD', 'Nado')  # 钉钉机器人关键词
