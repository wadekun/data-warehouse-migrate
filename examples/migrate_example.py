#!/usr/bin/env python3
"""
数据迁移示例脚本

演示如何使用DataMigrator类进行数据迁移
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from data_warehouse_migrate.migrator import DataMigrator, MigrationMode
from data_warehouse_migrate.logger import setup_logger

logger = setup_logger(__name__)


def main():
    """主函数"""
    
    # 配置参数
    config = {
        'source_project_id': 'your-maxcompute-project',
        'source_table_name': 'your_source_table',
        'destination_project_id': 'your-bigquery-project',
        'destination_dataset_id': 'your_dataset',
        'destination_table_name': 'your_target_table',
        'mode': MigrationMode.APPEND,
        'batch_size': 5000,
        
        # 可选：如果不通过环境变量设置
        'maxcompute_access_id': os.getenv('MAXCOMPUTE_ACCESS_ID'),
        'maxcompute_secret_key': os.getenv('MAXCOMPUTE_SECRET_ACCESS_KEY'),
        'maxcompute_endpoint': os.getenv('MAXCOMPUTE_ENDPOINT'),
        'bigquery_credentials_path': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
    }
    
    try:
        logger.info("开始数据迁移示例")
        
        # 创建迁移器
        migrator = DataMigrator(
            source_project_id=config['source_project_id'],
            destination_project_id=config['destination_project_id'],
            maxcompute_access_id=config['maxcompute_access_id'],
            maxcompute_secret_key=config['maxcompute_secret_key'],
            maxcompute_endpoint=config['maxcompute_endpoint'],
            bigquery_credentials_path=config['bigquery_credentials_path']
        )
        
        # 执行迁移
        migrator.migrate_table(
            source_table_name=config['source_table_name'],
            destination_dataset_id=config['destination_dataset_id'],
            destination_table_name=config['destination_table_name'],
            mode=config['mode'],
            batch_size=config['batch_size']
        )
        
        logger.info("数据迁移完成")
        
    except Exception as e:
        logger.error(f"数据迁移失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
