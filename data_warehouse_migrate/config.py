"""
配置管理模块
"""

import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    # 查找并加载 .env 文件
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"已加载环境配置文件: {env_path}")
    else:
        # 尝试在当前工作目录查找
        current_env = Path.cwd() / '.env'
        if current_env.exists():
            load_dotenv(current_env)
            print(f"已加载环境配置文件: {current_env}")
except ImportError:
    print("警告: python-dotenv 未安装，无法自动加载 .env 文件")


class Config:
    """配置类"""
    
    def __init__(self):
        # MaxCompute配置
        self.maxcompute_access_id = os.getenv('MAXCOMPUTE_ACCESS_ID')
        self.maxcompute_secret_access_key = os.getenv('MAXCOMPUTE_SECRET_ACCESS_KEY')
        self.maxcompute_endpoint = os.getenv('MAXCOMPUTE_ENDPOINT')
        
        # BigQuery配置
        self.bigquery_credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        # 日志配置
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        
    def validate_maxcompute_config(self) -> bool:
        """验证MaxCompute配置"""
        return all([
            self.maxcompute_access_id,
            self.maxcompute_secret_access_key,
            self.maxcompute_endpoint
        ])
    
    def validate_bigquery_config(self) -> bool:
        """验证BigQuery配置"""
        return bool(self.bigquery_credentials_path and os.path.exists(self.bigquery_credentials_path))


# 全局配置实例
config = Config()
