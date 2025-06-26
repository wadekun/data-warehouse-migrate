"""
日志配置模块
"""

import logging
import sys
from typing import Optional

from .config import config


def setup_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """设置日志记录器"""
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    # 设置日志级别
    log_level = level or config.log_level
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logger.level)
    
    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger


# 默认日志记录器
logger = setup_logger(__name__)
