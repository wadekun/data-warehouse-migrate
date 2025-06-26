#!/usr/bin/env python3
"""
连接测试脚本

用于测试MaxCompute和BigQuery的连接配置
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_warehouse_migrate.maxcompute_client import MaxComputeClient
from data_warehouse_migrate.bigquery_client import BigQueryClient
from data_warehouse_migrate.config import config
from data_warehouse_migrate.logger import setup_logger

logger = setup_logger(__name__)


def test_maxcompute_connection(project_id: str):
    """测试MaxCompute连接"""
    logger.info("测试MaxCompute连接...")
    
    try:
        client = MaxComputeClient(project=project_id)
        
        if client.test_connection():
            logger.info("✓ MaxCompute连接成功")
            
            # 尝试列出一些表
            try:
                tables = list(client.odps.list_tables(max_items=5))
                logger.info(f"项目中的表（前5个）: {[t.name for t in tables]}")
            except Exception as e:
                logger.warning(f"列出表时出错: {e}")
            
            return True
        else:
            logger.error("✗ MaxCompute连接失败")
            return False
            
    except Exception as e:
        logger.error(f"✗ MaxCompute连接测试出错: {e}")
        return False


def test_bigquery_connection(project_id: str):
    """测试BigQuery连接"""
    logger.info("测试BigQuery连接...")
    
    try:
        client = BigQueryClient(project_id=project_id)
        
        if client.test_connection():
            logger.info("✓ BigQuery连接成功")
            
            # 尝试列出一些数据集
            try:
                datasets = list(client.client.list_datasets(max_results=5))
                logger.info(f"项目中的数据集（前5个）: {[d.dataset_id for d in datasets]}")
            except Exception as e:
                logger.warning(f"列出数据集时出错: {e}")
            
            return True
        else:
            logger.error("✗ BigQuery连接失败")
            return False
            
    except Exception as e:
        logger.error(f"✗ BigQuery连接测试出错: {e}")
        return False


def main():
    """主函数"""
    logger.info("开始连接测试")
    logger.info("=" * 50)
    
    # 检查环境变量
    logger.info("检查环境变量配置...")
    
    # MaxCompute配置
    maxcompute_config_ok = True
    if not config.maxcompute_access_id:
        logger.error("缺少环境变量: MAXCOMPUTE_ACCESS_ID")
        maxcompute_config_ok = False
    
    if not config.maxcompute_secret_access_key:
        logger.error("缺少环境变量: MAXCOMPUTE_SECRET_ACCESS_KEY")
        maxcompute_config_ok = False
    
    if not config.maxcompute_endpoint:
        logger.error("缺少环境变量: MAXCOMPUTE_ENDPOINT")
        maxcompute_config_ok = False
    
    # BigQuery配置
    bigquery_config_ok = True
    if not config.bigquery_credentials_path:
        logger.warning("未设置环境变量: GOOGLE_APPLICATION_CREDENTIALS")
        logger.warning("将尝试使用默认凭证")
    elif not os.path.exists(config.bigquery_credentials_path):
        logger.error(f"BigQuery凭证文件不存在: {config.bigquery_credentials_path}")
        bigquery_config_ok = False
    
    logger.info("=" * 50)
    
    # 获取项目ID
    maxcompute_project = input("请输入MaxCompute项目ID: ").strip()
    bigquery_project = input("请输入BigQuery项目ID: ").strip()
    
    if not maxcompute_project or not bigquery_project:
        logger.error("项目ID不能为空")
        sys.exit(1)
    
    logger.info("=" * 50)
    
    # 测试连接
    success_count = 0
    
    if maxcompute_config_ok:
        if test_maxcompute_connection(maxcompute_project):
            success_count += 1
    else:
        logger.error("跳过MaxCompute连接测试（配置不完整）")
    
    logger.info("-" * 30)
    
    if bigquery_config_ok:
        if test_bigquery_connection(bigquery_project):
            success_count += 1
    else:
        logger.error("跳过BigQuery连接测试（配置不完整）")
    
    logger.info("=" * 50)
    
    # 总结
    if success_count == 2:
        logger.info("🎉 所有连接测试通过！可以开始数据迁移。")
    elif success_count == 1:
        logger.warning("⚠️  部分连接测试通过，请检查失败的连接配置。")
    else:
        logger.error("❌ 所有连接测试失败，请检查配置。")
        sys.exit(1)


if __name__ == '__main__':
    main()
